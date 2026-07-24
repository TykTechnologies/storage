// Package aws implements a kv.Provider backed by AWS Secrets Manager.
//
// It resolves references of the form kv://<store>/<secret>[#field] (and the
// $kv{...} inline form) to a secret's payload, and adds new versions via Set.
// The provider holds one long-lived client and fetches over the network on
// each call, so it is always wrapped by the SecretStore cache and bounded by a
// per-call timeout: it implements Initializer, Setter and Timeouter, and
// deliberately not Standalone. There is no Closer — SDK v2 clients hold no
// closable resources.
//
// Authentication covers the ways a caller reaches AWS — the default credential
// chain (env vars, shared config, IMDS, IRSA; the default on AWS hosts), static
// access keys, a shared-config profile, and STS AssumeRole (with optional
// external ID) over any of those bases. Config documents the fields; Init maps
// them to SDK options.
//
// A store is scoped to one region: keys pass through verbatim (names may
// contain "/", full ARNs are accepted), but an ARN naming another region is
// rejected, not honored — references can be authored by untrusted parties.
// Cross-region access means configuring a second store. Cross-account ARNs are
// allowed; IAM decides. Version selection is store-level config
// (version_stage/version_id), not part of the key, because "/" is legal in
// secret names.
package aws
