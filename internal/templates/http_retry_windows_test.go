package templates

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

// Run the real PowerShell 5.1 functions against a local HTTP server. Never run
// the installer itself: extracting only functions avoids touching users/services.
func TestWindowsBootstrapHTTPRetry(t *testing.T) {
	content, err := GetTemplateContent(commonParams.Windows, params.GithubEndpointType)
	require.NoError(t, err)
	// Also exercise an operator's upgraded custom template before deployment.
	if custom := os.Getenv("GARM_TEST_WINDOWS_TEMPLATE"); custom != "" {
		content, err = os.ReadFile(custom) // #nosec G703 -- Explicit operator-supplied test fixture, never a production request.
		require.NoError(t, err)
	}
	rendered, err := RenderRunnerInstallScript(string(content), InstallContext{})
	require.NoError(t, err)
	templateFile := filepath.Join(t.TempDir(), "install.ps1")
	require.NoError(t, os.WriteFile(templateFile, rendered, 0o600)) // #nosec G703 -- Destination is a new testing.T.TempDir; only fixture contents are external.
	for _, tc := range []struct {
		name                       string
		status, failures, attempts int
		outFile, post              bool
	}{
		{"success", 200, 0, 1, false, false},
		{"origin-error", 520, 2, 3, false, false},
		{"credentials-file", 503, 1, 2, true, false},
		{"callback", 502, 1, 2, false, true},
		{"timeout", 408, 1, 2, false, false},
		{"throttled", 429, 1, 2, false, false},
		{"unauthorized", 401, 10, 1, false, false},
		{"not-found", 404, 10, 1, false, false},
		{"exhausted", 520, 10, 6, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := int(calls.Add(1))
				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("missing authorization header")
				}
				if tc.post && r.Method != http.MethodPost {
					t.Error("callback must remain POST")
				}
				if n <= tc.failures {
					w.WriteHeader(tc.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"value":"credentials"}`)
			}))
			defer server.Close()
			script := `param($TemplateFile, $Uri, $OutputFile)
$ErrorActionPreference = 'Stop'
$tokens = $null; $parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($TemplateFile, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw 'Invalid PowerShell template syntax' }
$names = @('Start-ExecuteWithRetry', 'Invoke-GarmWebRequest', 'Invoke-APICall')
$functions = $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -in $names }, $true)
if ($functions.Count -ne 3) { throw 'Missing HTTP retry functions' }
foreach ($function in $functions) { . ([scriptblock]::Create($function.Extent.Text)) }
function Start-Sleep { param($Seconds) }
$Token = 'test-token'
$request = @{ Uri=$Uri; Headers=@{ Authorization='Bearer test-token' } }
`
			if tc.outFile {
				script += "$request.OutFile = $OutputFile\n"
			}
			if tc.post {
				script += "Invoke-APICall -CallbackURL $Uri -Payload @{status='installing'}\n"
			} else {
				script += "$result = Invoke-GarmWebRequest @request\n"
				if tc.outFile {
					script += "$result = Get-Content -Raw $OutputFile\n"
				}
				script += "if (($result | ConvertFrom-Json).value -ne 'credentials') { throw 'Response polluted by retry output' }\n"
			}
			script += "if ($ErrorActionPreference -ne 'Stop') { throw 'Error preference leaked' }\n"
			scriptFile := filepath.Join(t.TempDir(), "test.ps1")
			require.NoError(t, os.WriteFile(scriptFile, []byte(script), 0o600))
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			output, runErr := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-File", scriptFile, templateFile, server.URL, filepath.Join(t.TempDir(), "credentials")).CombinedOutput()
			if tc.failures >= tc.attempts {
				require.Error(t, runErr)
			} else {
				require.NoError(t, runErr, string(output))
			}
			require.Equal(t, tc.attempts, int(calls.Load()), string(output))
			require.False(t, strings.Contains(string(output), "test-token"), "logs must not expose credentials")
		})
	}
}
