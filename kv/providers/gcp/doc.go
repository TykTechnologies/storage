// Package gcp implements a kv.Provider backed by Google Cloud Secret Manager.
//
// It resolves references of the form kv://<store>/<secret>[/versions/<n>][#field]
// (and the $kv{...} inline form) to a secret's payload, and adds new versions via
// Set. The provider holds one long-lived client and fetches over the network on
// each call, so it is always wrapped by the SecretStore cache and bounded by a
// per-call timeout: it implements Initializer, Closer, Setter and Timeouter, and
// deliberately not Standalone.
//
// Authentication covers the ways a caller reaches GCP — Application Default
// Credentials (the default on GCP hosts), an explicit service-account key,
// Workload Identity Federation, and service-account impersonation over any of
// those bases. Config documents the fields; Init maps them to client options.
//
// A store is scoped to one project (and optionally one region): every key
// resolves under the configured project_id, and a key naming another project is
// rejected, not honored — references can be authored by untrusted parties.
// Cross-project access means configuring a second store.
package gcp
