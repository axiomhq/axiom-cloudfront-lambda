package parser

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLogs(t *testing.T) {
	// read data from data/logs.tsv
	file, err := os.Open("testdata/logs.tsv")
	require.NoError(t, err)
	defer file.Close()

	// parse logs
	events, err := ParseCloudfrontLogs(file)
	require.NoError(t, err)

	assert.Len(t, events, 22)
}
