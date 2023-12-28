package gosdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HandlerFunc func(request interface{}) (interface{}, error)

type DryJob struct {
	ID      string                 `json:"id"`
	Request map[string]interface{} `json:"request"`
}

type RunConfig struct {
	Auth           map[string]interface{}            `json:"auth"`
	Inject         map[string]string                 `json:"inject"`
	DisableSecrets bool                              `json:"disableSecrets"`
	Swap           map[string]map[string]interface{} `json:"swap"`
}

func NewRunConfig() *RunConfig {
	return &RunConfig{
		Auth:           make(map[string]interface{}),
		Inject:         make(map[string]string),
		DisableSecrets: false,
		Swap:           make(map[string]map[string]interface{}),
	}
}

type DryJobError struct {
	StatusCode  int    `json:"status_code"`
	Description string `json:"description"`
	TimedOut    bool   `json:"timed_out"`
}

func NewDryJobError(statusCode int, description string) (*DryJobError, error) {
	if statusCode < 400 || statusCode > 599 {
		return nil, fmt.Errorf("status_code must be between 400 and 599")
	}
	return &DryJobError{
		StatusCode:  statusCode,
		Description: description,
		TimedOut:    false,
	}, nil
}

type DryClient struct {
	EngineHost             string
	ProxyIdentity          DryId
	ProxyHost              string
	ExecuteEndpoint        string
	HydrateEndpoint        string
	DeleteEndpoint         string
	RegisterProxyEndpoint  string
	SubmitJobProxyEndpoint string
	TenantSessionEndpoint  string
	GetJobsProxyEndpoint   string
	GetStateEndpoint       string
	ConnectEndpoint        string
	UpsertSecretEndpoint   string
	DeleteSecretEndpoint   string
	SyncEndpoint           string
	OauthKeyEndpoint       string
	APIKey                 string
	Verbose                bool
	Handlers               map[string]HandlerFunc
}

func NewDryClient(apiKey string, verbose bool, proxyIdentity *DryId, engineHost string, proxyHost string) *DryClient {
	if proxyIdentity == nil {
		proxyIdentity, _ = NewDryId("worker", "queue", nil, nil, nil)
	}
	if engineHost == "" {
		engineHost = "https://api.drymerge.com"
	}
	if proxyHost == "" {
		proxyHost = "https://proxy-srv.drymerge.com"
	}
	return &DryClient{
		EngineHost:             engineHost,
		ProxyIdentity:          *proxyIdentity,
		ProxyHost:              proxyHost,
		ExecuteEndpoint:        "/execute",
		HydrateEndpoint:        "/upsert-with-template",
		DeleteEndpoint:         "/remove-entities",
		RegisterProxyEndpoint:  "/hire/*proxy_identity",
		SubmitJobProxyEndpoint: "/retire/*job_id",
		GetJobsProxyEndpoint:   "/employ/*proxy_identity",
		TenantSessionEndpoint:  "/tenant/session",
		GetStateEndpoint:       "/state/*identity",
		ConnectEndpoint:        "/connect/*identity",
		UpsertSecretEndpoint:   "/secret/upsert",
		DeleteSecretEndpoint:   "/secret/remove",
		SyncEndpoint:           "/sync/*identity",
		OauthKeyEndpoint:       "/oauth/token/*identity",
		APIKey:                 apiKey,
		Verbose:                verbose,
		Handlers:               make(map[string]HandlerFunc),
	}
}

func (dc *DryClient) Run(dryId *DryId, runConfig *RunConfig, args map[string]interface{}) (map[string]interface{}, error) {
	if runConfig == nil {
		runConfig = NewRunConfig()
	}
	endpoint := fmt.Sprintf("%s%s/%s", dc.EngineHost, dc.ExecuteEndpoint, dryId.String())
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}

	payload := map[string]interface{}{
		"config": runConfig,
		"args":   args,
	}

	payloadBytes, _ := json.Marshal(payload)
	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	dc.report(fmt.Sprintf("Failed to run job with error: %s", string(body)))
	return nil, fmt.Errorf("failed with status code: %d", resp.StatusCode)
}

// Other methods (Template, Delete, RegisterProxy, etc.) would be implemented similarly.

func (dc *DryClient) doRequest(method string, url string, headers map[string]string, body *bytes.Reader) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = body
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (dc *DryClient) report(message string) {
	if dc.Verbose {
		log.Println(message)
	}
}

func (dc *DryClient) Template(dryId *DryId, source map[string]interface{}) (map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s%s", dc.EngineHost, dc.HydrateEndpoint)
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}

	payload := map[string]interface{}{
		"template": dryId.String(),
		"hydrate":  source,
	}

	payloadBytes, _ := json.Marshal(payload)
	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		dc.report(fmt.Sprintf("Successfully hydrated template %s", dryId))
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	dc.report(fmt.Sprintf("Failed to hydrate template with error: %s", string(body)))
	return nil, fmt.Errorf("failed with status code: %d", resp.StatusCode)
}

func (dc *DryClient) Delete(ids []*DryId) error {
	endpoint := fmt.Sprintf("%s%s", dc.EngineHost, dc.DeleteEndpoint)
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}

	entities := make([]string, len(ids))
	for i, id := range ids {
		entities[i] = id.String()
	}

	payload := map[string]interface{}{
		"entities": entities,
	}

	payloadBytes, _ := json.Marshal(payload)
	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		dc.report(fmt.Sprintf("Successfully deleted ids: %v", ids))
		return nil
	}

	dc.report(fmt.Sprintf("Failed to delete entities with error: %s", string(body)))
	return fmt.Errorf("failed with status code: %d", resp.StatusCode)
}

func (dc *DryClient) Route(url string, handler HandlerFunc) *DryClient {
	dc.Handlers[url] = handler
	return dc
}

func (dc *DryClient) ProcessJob(job *DryJob) error {
	handler, exists := dc.Handlers[job.Request["url"].(string)]
	if !exists {
		errMsg := fmt.Sprintf("No handler found for job %s with details %v", job.ID, job.Request)
		dc.report(errMsg)
		jobError, _ := NewDryJobError(404, errMsg)
		dc.SubmitJobResponse(job.ID, jobError)
		return fmt.Errorf(errMsg)
	}

	dc.report(fmt.Sprintf("[PROXY] Running handler for job %s with details %v", job.ID, job.Request))
	jobResponse, err := handler(job.Request["body"])
	if err != nil {
		errMsg := fmt.Sprintf("Error running handler for job %s with details %v: %v", job.ID, job.Request, err)
		dc.report(errMsg)
		jobError, _ := NewDryJobError(500, errMsg)
		dc.SubmitJobResponse(job.ID, jobError)
		return fmt.Errorf(errMsg)
	}

	dc.SubmitJobResponse(job.ID, jobResponse)
	return nil
}

func (dc *DryClient) SubmitJobResponse(jobId string, response interface{}) error {
	dc.report(fmt.Sprintf("[PROXY] Submitting job response for job %s", jobId))
	url := fmt.Sprintf("%s%s", dc.ProxyHost, strings.Replace(dc.SubmitJobProxyEndpoint, "*job_id", jobId, -1))
	dc.report(fmt.Sprintf("[PROXY] Submitting job response to %s", url))
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}
	structuredResult := map[string]interface{}{
		"Generic": response,
	}
	jsonBody := map[string]interface{}{
		"job_id": jobId,
		"result": structuredResult,
	}
	payloadBytes, _ := json.Marshal(jsonBody)
	resp, err := dc.doRequest("POST", url, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		dc.report(fmt.Sprintf("[PROXY] Successfully submitted job response for job %s", jobId))
		return nil
	}

	dc.report(fmt.Sprintf("[PROXY] Failed to submit job response for job %s", jobId))
	dc.report(string(body))
	return fmt.Errorf("failed to submit job response for job %s", jobId)
}

// ... (previous code)

func (dc *DryClient) Start() {
	dc.report("Starting DryProxy, a lightweight proxy server facilitating DryMerge workflows...")
	for {
		url := fmt.Sprintf("%s%s", dc.ProxyHost, strings.Replace(dc.GetJobsProxyEndpoint, "*proxy_identity", dc.ProxyIdentity.String(), -1))
		headers := map[string]string{"Authorization": "Bearer " + dc.APIKey}

		dc.report(fmt.Sprintf("[PROXY] Polling for jobs at %s", url))
		resp, err := dc.doRequest("GET", url, headers, nil)
		if err != nil {
			dc.report(fmt.Sprintf("[PROXY] Error polling for jobs: %v", err))
			time.Sleep(1 * time.Second) // Wait for a second before retrying
			continue
		}

		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode == 200 {
			var responseData struct {
				Jobs []struct {
					ID   string `json:"id"`
					Work struct {
						Computer map[string]interface{} `json:"Computer"`
					} `json:"work"`
				} `json:"jobs"`
			}
			if err := json.Unmarshal(body, &responseData); err != nil {
				dc.report(fmt.Sprintf("[PROXY] Error unmarshalling job data: %v", err))
				continue
			}

			var jobs []*DryJob
			for _, jobData := range responseData.Jobs {
				jobs = append(jobs, &DryJob{ID: jobData.ID, Request: jobData.Work.Computer})
			}
			dc.ProcessJobs(jobs)
		} else {
			dc.report(fmt.Sprintf("[PROXY] Job queue endpoint responded with error: %s", string(body)))
		}
	}
}

func (dc *DryClient) ProcessJobs(jobs []*DryJob) {
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(j *DryJob) {
			defer wg.Done()
			if err := dc.ProcessJob(j); err != nil {
				dc.report(fmt.Sprintf("[PROXY] Error processing job %s: %v", j.ID, err))
			}
		}(job)
	}
	wg.Wait()
}

func (dc *DryClient) TenantSession(tenantId string) (string, error) {
	if len(tenantId) > 32 {
		return "", fmt.Errorf("tenant ID must be less than or equal to 32 characters")
	}

	endpoint := fmt.Sprintf("%s%s", dc.EngineHost, dc.TenantSessionEndpoint)
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}

	payload := map[string]string{"tenant_id": tenantId}
	payloadBytes, _ := json.Marshal(payload)

	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		var data map[string]string
		if err := json.Unmarshal(body, &data); err != nil {
			return "", err
		}
		dc.report(fmt.Sprintf("Successfully retrieved tenant session for tenant %s", tenantId))
		return data["session_key"], nil
	}

	dc.report(fmt.Sprintf("Failed to retrieve tenant session for tenant %s with error: %s", tenantId, string(body)))
	return "", fmt.Errorf("failed with status code: %d", resp.StatusCode)
}

func (dc *DryClient) State(dryId *DryId) (map[string]interface{}, error) {
	dc.report(fmt.Sprintf("Retrieving state for trigger %s", dryId.String()))
	endpoint := fmt.Sprintf("%s%s/%s", dc.EngineHost, dc.GetStateEndpoint, dryId.String())
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
	}

	resp, err := dc.doRequest("GET", endpoint, headers, nil)
	if err != nil {
		dc.report(fmt.Sprintf("Failed to retrieve state for trigger %s with error: %v", dryId, err))
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 200 {
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		dc.report(fmt.Sprintf("Successfully retrieved state for trigger %s", dryId))
		return result, nil
	}

	dc.report(fmt.Sprintf("Failed to retrieve state for trigger %s with error: %s", dryId, string(body)))
	return nil, fmt.Errorf("failed with status code: %d", resp.StatusCode)
}

func (dc *DryClient) UpsertSecret(name, value, namespace, tenantId string) error {
	payload := map[string]interface{}{
		"secret_name":      name,
		"secret_value":     value,
		"secret_namespace": namespace,
		"secret_tenant":    tenantId,
	}
	payloadBytes, _ := json.Marshal(payload)
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}
	endpoint := dc.EngineHost + dc.UpsertSecretEndpoint
	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (dc *DryClient) DeleteSecret(name, namespace, tenantId string) error {
	payload := map[string]interface{}{
		"secret_name":      name,
		"secret_namespace": namespace,
		"secret_tenant":    tenantId,
	}
	payloadBytes, _ := json.Marshal(payload)
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
		"Content-Type":  "application/json",
	}
	endpoint := dc.EngineHost + dc.DeleteSecretEndpoint
	resp, err := dc.doRequest("POST", endpoint, headers, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (dc *DryClient) SyncInfra(triggerOrCronId *DryId) error {
	endpoint := fmt.Sprintf("%s%s/%s", dc.EngineHost, dc.SyncEndpoint, triggerOrCronId.String())
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
	}
	resp, err := dc.doRequest("POST", endpoint, headers, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (dc *DryClient) OAuthToken(connectionId *DryId) (string, error) {
	endpoint := fmt.Sprintf("%s%s/%s", dc.EngineHost, dc.OauthKeyEndpoint, connectionId.String())
	headers := map[string]string{
		"Authorization": "Bearer " + dc.APIKey,
	}

	resp, err := dc.doRequest("GET", endpoint, headers, nil)
	if err != nil {
		dc.report(fmt.Sprintf("Failed to retrieve OAuth token for connection %s with error: %v", connectionId, err))
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return "", err
		}
		token, ok := result["access_token"].(string)
		if !ok {
			return "", fmt.Errorf("access_token not found in response")
		}
		dc.report(fmt.Sprintf("Successfully retrieved OAuth token for connection %s", connectionId))
		return token, nil
	}

	dc.report(fmt.Sprintf("Failed to retrieve OAuth token for connection %s with status code %d: %s", connectionId, resp.StatusCode, string(body)))
	return "", fmt.Errorf("HTTP Error: %d", resp.StatusCode)
}
