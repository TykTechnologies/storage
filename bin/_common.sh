# listPackages lists local go packages under the "persistent" directory.
function listPackages {
    go list ./persistent/...
}
