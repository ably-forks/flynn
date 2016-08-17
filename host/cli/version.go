package cli

import (
	"fmt"

	"github.com/ably-forks/flynn/pkg/version"
)

func init() {
	Register("version", runVersion, `
usage: flynn-host version

Show current version`)
}

func runVersion() {
	fmt.Println(version.String())
}
