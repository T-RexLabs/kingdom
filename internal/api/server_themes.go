package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/theme"
)

func (s *Server) registerThemeRoutes(mux *http.ServeMux) {
	routes := map[string]http.HandlerFunc{
		"GET /themes":                                          s.handleListThemes,
		"GET /themes/schema":                                   s.handleThemeSchema,
		"POST /themes/validate":                                s.handleValidateTheme,
		"POST /themes/validate-archive":                        s.handleValidateThemeArchive,
		"POST /themes/import":                                  s.handleImportTheme,
		"GET /themes/{id}/source":                              s.handleGetThemeSource,
		"PUT /themes/{id}/source":                              s.handleSaveThemeSource,
		"DELETE /themes/{id}/source":                           s.handleDeleteThemeSource,
		"POST /themes/{id}/publish":                            s.handlePublishTheme,
		"DELETE /themes/{id}":                                  s.handleRemoveTheme,
		"GET /themes/{id}/revisions/{digest}":                  s.handleGetThemeRevision,
		"GET /themes/{id}/revisions/{digest}/assets/{assetId}": s.handleGetThemeAsset,
		"GET /themes/{id}/revisions/{digest}/export":           s.handleExportTheme,
	}
	for route, handler := range routes {
		mux.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			if _, _, ok := s.requirePrincipal(w, r, "api.themes"); !ok {
				return
			}
			if s.themes == nil {
				writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeDependencyBlocked, "theme storage is unavailable", "api.themes", nil)
				return
			}
			handler(w, r)
		})
	}
}

func (s *Server) writeThemeError(w http.ResponseWriter, err error) {
	var validation *theme.ValidationError
	switch {
	case errors.As(err, &validation):
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "The theme could not be validated. Review the highlighted fields.", "api.themes", map[string]any{"diagnostics": validation.Diagnostics})
	case errors.Is(err, theme.ErrNotFound):
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "theme source or revision not found", "api.themes", nil)
	case errors.Is(err, theme.ErrConflict):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "The theme changed since you opened it. Keep your draft and reload the current source before saving.", "api.themes", nil)
	case errors.Is(err, theme.ErrBuiltin):
		writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "Built-in themes cannot be changed. Create a copy with a new theme ID.", "api.themes", nil)
	default:
		s.writeInternalError(w, "theme storage operation failed")
	}
}

// Base64 assets plus escaped manifest text have a bounded wire budget. This
// middleware runs after authentication, before spooling and schema validation.
const maxThemeRequestBytes = int64(theme.MaxPackageBytes*4/3 + theme.MaxManifestBytes*6 + 1<<20)

func limitThemeRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/themes/") && r.Body != nil {
			if r.ContentLength > maxThemeRequestBytes {
				writeCatalogError(w, http.StatusRequestEntityTooLarge, errorcatalog.CodePolicyViolation, "Theme upload exceeds the package size limit.")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxThemeRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func decodeThemeRequest(_ http.ResponseWriter, r *http.Request, target any) error {
	return decodeStrictJSON(r, target)
}

func (s *Server) handleListThemes(w http.ResponseWriter, r *http.Request) {
	catalog, err := s.themes.List()
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, catalog)
}

func (s *Server) handleThemeSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(theme.SchemaJSON())
}

func (s *Server) handleValidateTheme(w http.ResponseWriter, r *http.Request) {
	var request gen.ThemeDraftRequest
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	resolved, err := s.themes.Resolver().Resolve([]byte(request.Source), request.Assets)
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resolved)
}

// Archive validation shares the canonical bounded parser with import but
// never writes source files or publishes a catalog revision.
func (s *Server) handleValidateThemeArchive(w http.ResponseWriter, r *http.Request) {
	var request gen.ImportThemeJSONBody
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	resolved, err := s.themes.Resolver().ImportZIP(request.Archive)
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resolved)
}

func (s *Server) handleImportTheme(w http.ResponseWriter, r *http.Request) {
	var request gen.ImportThemeJSONBody
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	resolved, err := s.themes.ImportZIP(request.Archive)
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resolved)
}

func (s *Server) handleGetThemeRevision(w http.ResponseWriter, r *http.Request) {
	resolved, err := s.themes.ReadRevision(r.PathValue("id"), r.PathValue("digest"))
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resolved)
}

func presentThemeSource(source theme.Source) gen.ThemeSource {
	assets := source.Assets
	if assets == nil {
		assets = map[string][]byte{}
	}
	result := gen.ThemeSource{Source: source.Content, SourceDigest: source.SourceDigest, Assets: assets}
	if len(source.Diagnostics) > 0 {
		diagnostics := make([]gen.ThemeDiagnostic, 0, len(source.Diagnostics))
		for _, d := range source.Diagnostics {
			diagnostics = append(diagnostics, gen.ThemeDiagnostic{Path: d.Path, Code: d.Code, Message: d.Message})
		}
		result.Diagnostics = &diagnostics
	}
	return result
}

func (s *Server) handleGetThemeSource(w http.ResponseWriter, r *http.Request) {
	if digest := r.URL.Query().Get("revision"); digest != "" {
		source, err := s.themes.ReadRevisionSource(r.PathValue("id"), digest)
		if err != nil {
			s.writeThemeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, presentThemeSource(source))
		return
	}
	source, err := s.themes.ReadSource(r.PathValue("id"))
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, presentThemeSource(source))
}

func (s *Server) handleDeleteThemeSource(w http.ResponseWriter, r *http.Request) {
	var request gen.DeleteThemeSourceJSONBody
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	if err := s.themes.RemoveSource(r.PathValue("id"), request.ExpectedSourceDigest); err != nil {
		s.writeThemeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSaveThemeSource(w http.ResponseWriter, r *http.Request) {
	var request gen.ThemeSaveRequest
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	var identity struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(request.Source), &identity); err != nil {
		s.writeBadRequest(w, "Theme source must be a valid JSON document.")
		return
	}
	if identity.ID != r.PathValue("id") {
		s.writeBadRequest(w, "Theme ID must match the source being saved.")
		return
	}
	source, err := s.themes.SaveSource([]byte(request.Source), request.Assets, request.ExpectedSourceDigest)
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, presentThemeSource(source))
}

func (s *Server) handlePublishTheme(w http.ResponseWriter, r *http.Request) {
	var request gen.PublishThemeJSONBody
	if err := decodeThemeRequest(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	source, err := s.themes.ReadSource(r.PathValue("id"))
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	if source.SourceDigest != request.ExpectedSourceDigest {
		s.writeThemeError(w, theme.ErrConflict)
		return
	}
	// Publish exactly the source the author reviewed, even if another editor
	// saves a newer source while this immutable revision is being validated.
	resolved, err := s.themes.Publish(source.Manifest, source.Assets)
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resolved)
}

func (s *Server) handleRemoveTheme(w http.ResponseWriter, r *http.Request) {
	if err := s.themes.Remove(r.PathValue("id")); err != nil {
		s.writeThemeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetThemeAsset(w http.ResponseWriter, r *http.Request) {
	resolved, err := s.themes.ReadRevision(r.PathValue("id"), r.PathValue("digest"))
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	asset, found := resolved.Manifest.Assets[r.PathValue("assetId")]
	if !found {
		s.writeThemeError(w, theme.ErrNotFound)
		return
	}
	w.Header().Set("Content-Type", asset.MediaType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	_, _ = w.Write(resolved.Assets[asset.Path])
}

func (s *Server) handleExportTheme(w http.ResponseWriter, r *http.Request) {
	archive, err := s.themes.ExportZIP(r.PathValue("id"), r.PathValue("digest"))
	if err != nil {
		s.writeThemeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+r.PathValue("id")+`.zip"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(archive)
}
