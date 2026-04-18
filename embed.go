package mongolite

import "embed"

// SkillFS holds the mongolite Claude Code skill files.
// The "all:" prefix is required because the path starts with a dot.
//
//go:embed all:.claude/skills/mongolite
var SkillFS embed.FS
