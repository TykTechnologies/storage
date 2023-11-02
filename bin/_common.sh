# listPackages lists local go packages. In case a subpackage contains
# it's own go.mod file, it will not be listed as part of the output.
function listPackages {
  root=$1
	go list ./$root/... | grep -v "/temporal/"
}
