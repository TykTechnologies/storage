package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfined(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		base   string
		target string
		want   bool
	}{
		{"direct child", "/base", "/base/secret", true},
		{"nested child", "/base", "/base/sub/dir/secret", true},
		{"base itself", "/base", "/base", true},
		{"cleaned dotdot stays inside", "/base", "/base/sub/../secret", true},
		{"parent escape", "/base", "/base/../secret", false},
		{"sibling escape", "/base", "/other", false},
		{"prefix confusion is not confinement", "/base", "/base-evil", false},
		{"absolute outside", "/base", "/etc/passwd", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, confined(tc.base, tc.target))
		})
	}
}
