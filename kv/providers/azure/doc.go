// Package azure implements a kv.Provider backed by Azure Key Vault secrets, using the
// azsecrets data-plane client. A store of type azure_key_vault resolves references of the
// form kv://<store>/<secret>[/<version>] (or $kv{<store>:<secret>[/<version>]}): <version>
// pins an immutable snapshot, and a #<field> fragment — applied by the resolver, not this
// provider — selects a key from a JSON value.
//
// The provider is remote and cached. It is not Standalone, so the registry wraps it in the
// SecretStore for caching, singleflight, and a per-call timeout. It implements:
//
//   - KeyValueRetriever — Get the current enabled version, or a pinned one.
//   - Setter            — Set writes a new version; azsecrets.SetSecret is a single-call
//     upsert that creates the secret on first write. Set runs outside the SecretStore
//     wrapper, so it bounds its own deadline.
//   - Timeouter         — reports the configured per-call timeout.
//
// It is not an Initializer or Closer: azsecrets.NewClient and the azidentity credential
// constructors take no context and do no network I/O, so the credential and client are built
// once in the factory — nothing to defer, nothing to close. Auth failures surface on the
// first Get or Set and recover on their own.
//
// Authentication uses one of four explicit modes: managed_identity, workload_identity,
// client_secret, or client_certificate. DefaultAzureCredential and the CLI/developer
// credentials are rejected.
package azure
