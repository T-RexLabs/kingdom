package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	middleware "github.com/oapi-codegen/nethttp-middleware"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
)

func NewOpenAPIRequestValidator() (func(http.Handler) http.Handler, error) {
	swagger, err := loadOpenAPISpec()
	if err != nil {
		return nil, fmt.Errorf("load OpenAPI spec: %w", err)
	}

	swagger.Servers = nil

	validatorOpts := &middleware.Options{
		Options: openapi3filter.Options{
			AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
				if input == nil || input.SecurityScheme == nil || input.RequestValidationInput == nil || input.RequestValidationInput.Request == nil {
					return fmt.Errorf("missing authentication context")
				}

				if _, ok := authenticatedPrincipal(ctx); !ok {
					return fmt.Errorf("missing authenticated principal")
				}

				return nil
			},
		},
		ErrorHandlerWithOpts: func(ctx context.Context, err error, w http.ResponseWriter, r *http.Request, opts middleware.ErrorHandlerOpts) {
			statusCode := opts.StatusCode
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				statusCode = http.StatusRequestEntityTooLarge
			}
			code := validationErrorCodeForStatus(statusCode)
			correlationID := "openapi-validator"
			if route := opts.MatchedRoute; route != nil && route.Route != nil {
				correlationID = "openapi-validator:" + route.Route.Path
			}

			writeCanonicalError(
				w,
				statusCode,
				code,
				err.Error(),
				correlationID,
				map[string]any{"path": r.URL.Path, "method": r.Method},
			)
		},
		DoNotValidateServers: true,
	}

	return middleware.OapiRequestValidatorWithOptions(swagger, validatorOpts), nil
}

func loadOpenAPISpec() (*openapi3.T, error) {
	return gen.GetSwagger()
}

func validationErrorCodeForStatus(statusCode int) errorcatalog.Code {
	switch {
	case statusCode >= http.StatusInternalServerError:
		return errorcatalog.CodeInternalError
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return errorcatalog.CodePolicyViolation
	case statusCode == http.StatusNotFound:
		return errorcatalog.CodeDependencyBlocked
	default:
		return errorcatalog.CodePolicyViolation
	}
}
