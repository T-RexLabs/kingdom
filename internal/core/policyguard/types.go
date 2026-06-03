package policyguard

type RolePolicy struct {
	Version                 string              `yaml:"version"`
	PolicyID                string              `yaml:"policy_id"`
	Mode                    string              `yaml:"mode"`
	ForbiddenCommandClasses []string            `yaml:"forbidden_command_classes"`
	PathScopes              map[string]string   `yaml:"path_scopes"`
	Roles                   map[string]RoleRule `yaml:"roles"`
}

type RoleRule struct {
	ExecutionAuthority    string   `yaml:"execution_authority"`
	AllowedToolClasses    []string `yaml:"allowed_tool_classes"`
	AllowedCommandClasses []string `yaml:"allowed_command_classes"`
	AllowedPathScopes     []string `yaml:"allowed_path_scopes"`
}

type NetworkPolicy struct {
	Version      string                     `yaml:"version"`
	PolicyID     string                     `yaml:"policy_id"`
	Mode         string                     `yaml:"mode"`
	GlobalDenies NetworkGlobalDenies        `yaml:"global_denies"`
	Roles        map[string]NetworkRoleRule `yaml:"roles"`
}

type NetworkGlobalDenies struct {
	CIDRBlocks []string `yaml:"cidr_blocks"`
	Hostnames  []string `yaml:"hostnames"`
}

type NetworkRoleRule struct {
	DefaultAllowedDomains []string            `yaml:"default_allowed_domains"`
	NodeClassOverrides    map[string][]string `yaml:"node_class_overrides"`
}
