// Package hostedbuilder owns project database and provider assembly for the
// central host. It supplies the initialized managed provider required by projectruntime.
//
// CatalogDB, Connections, Registry, and model catalog lifecycle belong to the
// caller. RequestKey must return an owned 32-byte per-project key; Build clears
// the returned slice after constructing encrypted request storage. RootDir and
// all descendants are operator-owned, inaccessible to workloads and browsers.
//
// Each project database incarnation receives a distinct, retained native
// workspace. Uncertain creation may replay only its original request within the
// gateway's retention window. Lost database identity or expired replay remains
// unresolved and cannot allocate another workspace under the same mapping.
package hostedbuilder
