//go:build mage
// +build mage

package main

import (
	"github.com/aserto-dev/mage-loot/common"
	"github.com/aserto-dev/mage-loot/deps"
)

func Deps() {
	deps.GetAllDeps()
}

// Lint runs linting for the entire project.
func Lint() error {
	return common.Lint()
}

// Test runs all tests and generates a code coverage report.
func Test() error {
	return common.Test()
}
