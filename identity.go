package gosdk

import (
	"fmt"
	"regexp"
	"strings"
)

// IdentityError represents an identity-related error.
type IdentityError struct {
	msg string
}

func (e *IdentityError) Error() string {
	return e.msg
}

// NewIdentityError creates a new IdentityError with the provided message.
func NewIdentityError(msg string) *IdentityError {
	return &IdentityError{msg: msg}
}

// DryId represents a unique identifier.
type DryId struct {
	name         string
	namespace    *string
	organization *string
	type_        string
	version      *int
}

// ValidTypes is a set of valid identifier types.
var ValidTypes = map[string]struct{}{
	"queue": {}, "api": {}, "merge": {}, "state": {}, "route": {}, "fragment": {},
	"fn": {}, "trigger": {}, "cron": {}, "template": {}, "import": {}, "task": {},
}

// NewDryId creates a new DryId.
func NewDryId(name, type_ string, namespace, organization *string, version *int) (*DryId, error) {
	if !isValidString(name) ||
		(namespace != nil && !isValidString(*namespace)) ||
		(organization != nil && !isValidString(*organization)) {
		return nil, NewIdentityError("Invalid characters in name, namespace, or organization.")
	}

	if _, ok := ValidTypes[type_]; !ok {
		return nil, NewIdentityError(fmt.Sprintf("Invalid type: %s. Must be one of: %v", type_, keys(ValidTypes)))
	}

	return &DryId{
		name:         name,
		namespace:    namespace,
		organization: organization,
		type_:        type_,
		version:      version,
	}, nil
}

func (d *DryId) String() string {
	namespaceStr := "~/"
	if d.namespace != nil {
		namespaceStr = *d.namespace + "/"
	}
	organizationStr := ""
	if d.organization != nil {
		organizationStr = *d.organization + "/"
	}
	versionStr := ""
	if d.version != nil {
		versionStr = fmt.Sprintf(":%d", *d.version)
	}
	typeStr := fmt.Sprintf(".%s", d.type_)
	return fmt.Sprintf("%s%s%s%s%s", organizationStr, namespaceStr, d.name, typeStr, versionStr)
}

func isValidString(value string) bool {
	return !strings.ContainsAny(value, "/:.")
}

func ParseIdentityString(idStr string) (*DryId, error) {
	parts := strings.Split(idStr, "/")
	if len(parts) != 3 {
		return nil, NewIdentityError(fmt.Sprintf("Invalid identity string: %s", idStr))
	}

	organization := parts[0]
	namespace := parts[1]
	remainder := parts[2]

	name, type_, version, err := parseRemainder(remainder)
	if err != nil {
		return nil, err
	}

	return NewDryId(name, type_, &namespace, &organization, version)
}

func parseRemainder(remainder string) (string, string, *int, error) {
	re := regexp.MustCompile(`^([^.]+)\.([^:]+)(?::(\d+))?$`)
	matches := re.FindStringSubmatch(remainder)
	if matches == nil || len(matches) < 3 {
		return "", "", nil, NewIdentityError(fmt.Sprintf("Invalid remainder format: %s", remainder))
	}

	name := matches[1]
	type_ := matches[2]
	version := -1
	if len(matches) > 3 && matches[3] != "" {
		fmt.Sscanf(matches[3], "%d", &version)
	}

	if version == -1 {
		return name, type_, nil, nil
	}
	return name, type_, &version, nil
}

func keys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
