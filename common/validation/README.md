# RPC validation

Set `option (temporalvalidate.v1.rpc_validation).enabled = true;` once on an API
RPC. This enrolls both its request and response. Unenrolled RPCs need no exclusion
annotation. `rpc_validation.ignored` documents an exclusion for the whole RPC.

Requests are checked before the handler runs. Failures return InvalidArgument.
Responses are checked only after a successful handler call. Failures call
`logger.Warn(...)` and record a metric; the handler result is returned unchanged.

Static and nested checks require no owner-specific Server code. Symbols declared
in API's `temporalvalidate/v1/rules.proto` select methods from the generated
API-Go `Validator[ValidationContext]`
interface. API lint and the plugin enforce canonical reuse and declared types.
Add new methods to the existing frontend validator. Missing methods
or wrong signatures do not compile. `ValidationContext.Request` contains the
original request, also for nested fields and response checks. Authors extract
namespace from that request when needed.

See the [API authoring guide](https://github.com/long-nt-tran/api/blob/proto-annotations/temporalvalidate/README.md)
for the complete proto and Server examples.

Run from the Server repository root after updating the API-Go dependency:

```sh
go generate ./common/validation
make check-request-validation
```
