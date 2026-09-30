/**
 * Where every GitHub manifest callback lands.
 *
 * Kept separate from the page component so the router can name the path
 * without pulling the page into the initial bundle. The Go callbacks redirect
 * here (`githubSetupReturnPath` in internal/api/server_github_manifest.go).
 */
export const GITHUB_SETUP_RETURN_PATH = "/connections/github/return";
