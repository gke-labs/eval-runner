// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package terminalbench

import (
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

type taskFixtureEnvironment struct {
	temporaryDirectory      string
	workingDirectory        string
	taskDirectory           string
	instructionContent      string
	logsDirectory           string
	outputDirectory         string
	targetSolutionDirectory string
	targetTestsDirectory    string
	runTaskScript           string
}

func newTaskFixtureEnvironment(t *testing.T, taskName string, testScriptBody string, instructionBody string) *taskFixtureEnvironment {
	t.Helper()

	temporaryDirectory := t.TempDir()
	workingDirectory := filepath.Join(temporaryDirectory, "work")
	if err := os.MkdirAll(workingDirectory, 0755); err != nil {
		t.Fatalf("failed to create working directory: %v", err)
	}

	taskDirectory := filepath.Join(temporaryDirectory, taskName)
	solutionDirectory := filepath.Join(taskDirectory, "solution")
	testsDirectory := filepath.Join(taskDirectory, "tests")
	if err := os.MkdirAll(solutionDirectory, 0755); err != nil {
		t.Fatalf("failed to create solution directory: %v", err)
	}
	if err := os.MkdirAll(testsDirectory, 0755); err != nil {
		t.Fatalf("failed to create tests directory: %v", err)
	}

	solveScript := "#!/bin/bash\necho 'running solution'\n"
	if err := os.WriteFile(filepath.Join(solutionDirectory, "solve.sh"), []byte(solveScript), 0755); err != nil {
		t.Fatalf("failed to write solve.sh: %v", err)
	}

	if testScriptBody != "" {
		if err := os.WriteFile(filepath.Join(testsDirectory, "test.sh"), []byte(testScriptBody), 0755); err != nil {
			t.Fatalf("failed to write test.sh: %v", err)
		}
	}

	if instructionBody != "" {
		if err := os.WriteFile(filepath.Join(taskDirectory, "instruction.md"), []byte(instructionBody), 0644); err != nil {
			t.Fatalf("failed to write instruction.md: %v", err)
		}
	}

	logsDirectory := filepath.Join(temporaryDirectory, "logs")
	outputDirectory := filepath.Join(temporaryDirectory, "out")
	targetSolutionDirectory := filepath.Join(temporaryDirectory, "solution")
	targetTestsDirectory := filepath.Join(temporaryDirectory, "tests")

	runTaskScript, err := filepath.Abs("run-task.sh")
	if err != nil {
		t.Fatalf("failed to resolve run-task.sh path: %v", err)
	}

	return &taskFixtureEnvironment{
		temporaryDirectory:      temporaryDirectory,
		workingDirectory:        workingDirectory,
		taskDirectory:           taskDirectory,
		instructionContent:      instructionBody,
		logsDirectory:           logsDirectory,
		outputDirectory:         outputDirectory,
		targetSolutionDirectory: targetSolutionDirectory,
		targetTestsDirectory:    targetTestsDirectory,
		runTaskScript:           runTaskScript,
	}
}

func (environment *taskFixtureEnvironment) run(mode string, taskName string) ([]byte, error) {
	command := exec.Command("bash", environment.runTaskScript, taskName, mode, environment.workingDirectory)
	command.Dir = environment.temporaryDirectory
	command.Env = append(
		os.Environ(),
		"TASK_DIRECTORY="+environment.temporaryDirectory,
		"LOGS_DIRECTORY="+environment.logsDirectory,
		"OUT_DIRECTORY="+environment.outputDirectory,
		"SOLUTION_DIRECTORY="+environment.targetSolutionDirectory,
		"TESTS_DIRECTORY="+environment.targetTestsDirectory,
	)
	return command.CombinedOutput()
}

func (environment *taskFixtureEnvironment) readResults(t *testing.T) artifacts.ResultsFile {
	t.Helper()
	resultsPath := filepath.Join(environment.outputDirectory, "results.json")
	resultsBytes, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("failed to read results.json: %v", err)
	}
	var results artifacts.ResultsFile
	if err := json.Unmarshal(resultsBytes, &results); err != nil {
		t.Fatalf("failed to parse results.json: %v\ncontent: %s", err, string(resultsBytes))
	}
	return results
}

func runScriptInEnvironment(t *testing.T, taskName string, testScriptBody string) artifacts.ResultsFile {
	t.Helper()

	environment := newTaskFixtureEnvironment(t, taskName, testScriptBody, "Oracle test instruction\n")
	output, err := environment.run("oracle", taskName)
	if err != nil {
		t.Fatalf("run-task.sh failed: %v\noutput: %s", err, string(output))
	}

	return environment.readResults(t)
}

func TestRunTask(t *testing.T) {
	testCases := []struct {
		name            string
		testScript      string
		wantPassed      bool
		wantScore       float64
		wantMessagePart string
	}{
		{
			name: "reward 1 passes",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"echo 1 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n",
			wantPassed: true,
			wantScore:  1.0,
		},
		{
			name: "reward 0 fails with verifier log",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"echo 'assertion error in output'\n" +
				"echo 0 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n",
			wantPassed:      false,
			wantScore:       0.0,
			wantMessagePart: "assertion error in output",
		},
		{
			name: "missing reward fails with message",
			testScript: "#!/bin/bash\n" +
				"echo 'verifier crashed before producing reward'\n" +
				"exit 1\n",
			wantPassed:      false,
			wantScore:       0.0,
			wantMessagePart: "did not write reward",
		},
		{
			name: "non-numeric reward fails",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"echo 'not-a-number' > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n",
			wantPassed:      false,
			wantScore:       0.0,
			wantMessagePart: "non-numeric reward",
		},
		{
			name: "ansi color codes stripped cleanly in results JSON",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"printf '\\033[31mfailed with color\\033[0m\\n'\n" +
				"echo 0 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n",
			wantPassed:      false,
			wantScore:       0.0,
			wantMessagePart: "failed with color",
		},
		{
			name: "reward.json preferred over reward.txt",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"echo 0 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n" +
				"echo '{\"reward\": 1.0}' > \"${LOGS_DIRECTORY}/verifier/reward.json\"\n",
			wantPassed: true,
			wantScore:  1.0,
		},
		{
			name: "reward.json multi-metric dict parses reward key",
			testScript: "#!/bin/bash\n" +
				"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
				"echo '{\"accuracy\": 0.5, \"reward\": 1}' > \"${LOGS_DIRECTORY}/verifier/reward.json\"\n",
			wantPassed: true,
			wantScore:  1.0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			results := runScriptInEnvironment(t, "sample-task", testCase.testScript)
			if len(results.Samples) != 1 {
				t.Fatalf("expected 1 sample, got %d", len(results.Samples))
			}
			sample := results.Samples[0]
			if sample.Passed == nil || *sample.Passed != testCase.wantPassed {
				t.Errorf("Passed = %v, want %v (message: %s)", sample.Passed, testCase.wantPassed, sample.Message)
			}
			if sample.Score == nil || *sample.Score != testCase.wantScore {
				t.Errorf("Score = %v, want %v", sample.Score, testCase.wantScore)
			}
			if testCase.wantMessagePart != "" && !strings.Contains(sample.Message, testCase.wantMessagePart) {
				t.Errorf("Message %q does not contain %q", sample.Message, testCase.wantMessagePart)
			}
			if testCase.wantPassed && sample.Message != "" {
				t.Errorf("expected empty message on success, got %q", sample.Message)
			}
		})
	}
}

func TestRunTask_OracleExecutesSolution(t *testing.T) {
	environment := newTaskFixtureEnvironment(t, "solve-check-task", "", "Oracle solve check instruction\n")

	solveScript := "#!/bin/bash\necho 'solution marker' > solution_applied.marker\n"
	if err := os.WriteFile(filepath.Join(environment.taskDirectory, "solution", "solve.sh"), []byte(solveScript), 0755); err != nil {
		t.Fatalf("failed to write solve.sh: %v", err)
	}

	testScript := "#!/bin/bash\n" +
		"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
		"if [ -f solution_applied.marker ]; then\n" +
		"  echo 1 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n" +
		"else\n" +
		"  echo 'solution was not executed'\n" +
		"  echo 0 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n" +
		"fi\n"
	if err := os.WriteFile(filepath.Join(environment.taskDirectory, "tests", "test.sh"), []byte(testScript), 0755); err != nil {
		t.Fatalf("failed to write test.sh: %v", err)
	}

	output, err := environment.run("oracle", "solve-check-task")
	if err != nil {
		t.Fatalf("oracle run failed: %v\noutput: %s", err, string(output))
	}

	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || !*sample.Passed {
		t.Errorf("expected passed: true, got %v (message: %s)", sample.Passed, sample.Message)
	}
	if sample.Score == nil || *sample.Score != 1.0 {
		t.Errorf("expected score: 1.0, got %v", sample.Score)
	}
}

func TestRunTask_OracleMissingTestScriptExitsZero(t *testing.T) {
	environment := newTaskFixtureEnvironment(t, "missing-test-task", "", "Test instruction\n")
	// Remove test.sh so tests/ directory exists but test.sh is absent
	testScriptPath := filepath.Join(environment.taskDirectory, "tests", "test.sh")
	_ = os.Remove(testScriptPath)

	output, err := environment.run("oracle", "missing-test-task")
	if err != nil {
		t.Fatalf("oracle exited non-zero when test.sh was missing: %v\noutput: %s", err, string(output))
	}

	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "verifier did not write reward file: test script not found at") {
		t.Errorf("expected message to mention 'test script not found', got %q", sample.Message)
	}
}

func TestRunTask_Prepare(t *testing.T) {
	expectedInstruction := "## Task Instructions\nFix the bug in the code.\n"
	environment := newTaskFixtureEnvironment(t, "sample-task", "#!/bin/bash\necho 1 > reward.txt\n", expectedInstruction)

	output, err := environment.run("prepare", "sample-task")
	if err != nil {
		t.Fatalf("prepare failed: %v\noutput: %s", err, string(output))
	}

	// 1. Instruction written to OUT_DIRECTORY/instruction.md
	instructionPath := filepath.Join(environment.outputDirectory, "instruction.md")
	instructionBytes, err := os.ReadFile(instructionPath)
	if err != nil {
		t.Fatalf("expected instruction.md to exist: %v", err)
	}
	if string(instructionBytes) != expectedInstruction {
		t.Errorf("instruction.md = %q, want %q", string(instructionBytes), expectedInstruction)
	}

	// 2. Tests installed to TESTS_DIRECTORY
	installedTestScript := filepath.Join(environment.targetTestsDirectory, "test.sh")
	testBytes, err := os.ReadFile(installedTestScript)
	if err != nil {
		t.Fatalf("expected test.sh in tests directory: %v", err)
	}
	if !strings.Contains(string(testBytes), "reward.txt") {
		t.Errorf("test.sh content unexpected: %q", string(testBytes))
	}

	// 3. Solution directory must NOT be present
	if _, err := os.Stat(environment.targetSolutionDirectory); !os.IsNotExist(err) {
		t.Errorf("expected solution directory to not exist, but stat returned: %v", err)
	}

	// 4. results.json should NOT be written on successful prepare
	resultsPath := filepath.Join(environment.outputDirectory, "results.json")
	if _, err := os.Stat(resultsPath); !os.IsNotExist(err) {
		t.Errorf("expected results.json to not exist after successful prepare")
	}
}

func TestRunTask_PrepareFailure(t *testing.T) {
	environment := newTaskFixtureEnvironment(t, "sample-task", "#!/bin/bash\necho 1\n", "instruction")

	// Run prepare with a nonexistent task
	output, err := environment.run("prepare", "nonexistent-task")
	if err == nil {
		t.Fatalf("expected prepare for nonexistent task to fail, but succeeded\noutput: %s", string(output))
	}

	// Failure must write failing results.json and exit non-zero
	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "task directory not found") {
		t.Errorf("expected message to mention 'task directory not found', got %q", sample.Message)
	}
}

func TestRunTask_PrepareMissingTestScriptFails(t *testing.T) {
	environment := newTaskFixtureEnvironment(t, "missing-test-task", "", "Test instruction\n")
	testScriptPath := filepath.Join(environment.taskDirectory, "tests", "test.sh")
	_ = os.Remove(testScriptPath)

	output, err := environment.run("prepare", "missing-test-task")
	if err == nil {
		t.Fatalf("expected prepare to fail when test.sh is missing, but succeeded\noutput: %s", string(output))
	}

	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "task test script not found") {
		t.Errorf("expected message to mention 'task test script not found', got %q", sample.Message)
	}
}

func TestRunTask_PrepareFetchDeletesSolutions(t *testing.T) {
	taskName := "fetch-task"
	repositoryDirectory := t.TempDir()
	taskDirectory := filepath.Join(repositoryDirectory, taskName)
	solutionDirectory := filepath.Join(taskDirectory, "solution")
	testsDirectory := filepath.Join(taskDirectory, "tests")

	if err := os.MkdirAll(solutionDirectory, 0755); err != nil {
		t.Fatalf("failed to create solution directory: %v", err)
	}
	if err := os.MkdirAll(testsDirectory, 0755); err != nil {
		t.Fatalf("failed to create tests directory: %v", err)
	}

	expectedInstruction := "Instruction via fetch path\n"
	if err := os.WriteFile(filepath.Join(taskDirectory, "instruction.md"), []byte(expectedInstruction), 0644); err != nil {
		t.Fatalf("failed to write instruction.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(solutionDirectory, "solve.sh"), []byte("#!/bin/bash\necho solve\n"), 0755); err != nil {
		t.Fatalf("failed to write solve.sh: %v", err)
	}
	if err := os.WriteFile(filepath.Join(testsDirectory, "test.sh"), []byte("#!/bin/bash\necho 1 > reward.txt\n"), 0755); err != nil {
		t.Fatalf("failed to write test.sh: %v", err)
	}

	runGit := func(arguments ...string) string {
		command := exec.Command("git", arguments...)
		command.Dir = repositoryDirectory
		command.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + repositoryDirectory,
		}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s failed: %v\noutput: %s", strings.Join(arguments, " "), err, string(output))
		}
		return strings.TrimSpace(string(output))
	}

	runGit("init", "-b", "main")
	runGit("config", "user.email", "test@example.com")
	runGit("config", "user.name", "Test")
	runGit("add", ".")
	runGit("commit", "-m", "initial commit")
	commitHash := runGit("rev-parse", "HEAD")

	sandboxDirectory := t.TempDir()
	workingDirectory := filepath.Join(sandboxDirectory, "app")
	logsDirectory := filepath.Join(sandboxDirectory, "logs")
	outputDirectory := filepath.Join(sandboxDirectory, "out")
	targetSolutionDirectory := filepath.Join(sandboxDirectory, "solution")
	targetTestsDirectory := filepath.Join(sandboxDirectory, "tests")
	checkoutDirectory := filepath.Join(sandboxDirectory, "tmp", "checkout")

	runTaskScript, err := filepath.Abs("run-task.sh")
	if err != nil {
		t.Fatalf("failed to resolve run-task.sh path: %v", err)
	}

	command := exec.Command("bash", runTaskScript, taskName, "prepare", workingDirectory)
	command.Dir = sandboxDirectory
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + sandboxDirectory,
		"PINNED_COMMIT=" + commitHash,
		"REPOSITORY_URL=" + repositoryDirectory,
		"FETCH_CHECKOUT_DIRECTORY=" + checkoutDirectory,
		"LOGS_DIRECTORY=" + logsDirectory,
		"OUT_DIRECTORY=" + outputDirectory,
		"SOLUTION_DIRECTORY=" + targetSolutionDirectory,
		"TESTS_DIRECTORY=" + targetTestsDirectory,
	}

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("prepare via fetch failed: %v\noutput: %s", err, string(output))
	}

	// Verify instruction was written
	instructionBytes, err := os.ReadFile(filepath.Join(outputDirectory, "instruction.md"))
	if err != nil {
		t.Fatalf("failed to read instruction.md: %v", err)
	}
	if string(instructionBytes) != expectedInstruction {
		t.Errorf("instruction.md = %q, want %q", string(instructionBytes), expectedInstruction)
	}

	// Verify test script was installed
	if _, err := os.Stat(filepath.Join(targetTestsDirectory, "test.sh")); err != nil {
		t.Fatalf("test.sh was not installed: %v", err)
	}

	// Assert no solution directory or solve.sh exists anywhere within sandboxDirectory
	err = filepath.WalkDir(sandboxDirectory, func(path string, d fs.DirEntry, walkError error) error {
		if walkError != nil {
			return walkError
		}
		if d.IsDir() && d.Name() == "solution" {
			t.Errorf("found solution directory in sandbox: %s", path)
		}
		if !d.IsDir() && d.Name() == "solve.sh" {
			t.Errorf("found solve.sh file in sandbox: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk sandbox directory: %v", err)
	}
}

func TestRunTask_VerifyAfterPrepare(t *testing.T) {
	t.Run("passing reward 1", func(t *testing.T) {
		testScript := "#!/bin/bash\n" +
			"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
			"echo 1 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n"
		environment := newTaskFixtureEnvironment(t, "sample-task", testScript, "Test instruction")

		// 1. Run prepare
		prepareOutput, err := environment.run("prepare", "sample-task")
		if err != nil {
			t.Fatalf("prepare failed: %v\noutput: %s", err, string(prepareOutput))
		}

		// Solution must not exist after prepare
		if _, err := os.Stat(environment.targetSolutionDirectory); !os.IsNotExist(err) {
			t.Fatalf("expected solution directory to not exist after prepare")
		}

		// 2. Run verify
		verifyOutput, err := environment.run("verify", "sample-task")
		if err != nil {
			t.Fatalf("verify failed: %v\noutput: %s", err, string(verifyOutput))
		}

		results := environment.readResults(t)
		if len(results.Samples) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(results.Samples))
		}
		sample := results.Samples[0]
		if sample.Passed == nil || !*sample.Passed {
			t.Errorf("expected passed: true, got %v (message: %s)", sample.Passed, sample.Message)
		}
		if sample.Score == nil || *sample.Score != 1.0 {
			t.Errorf("expected score: 1.0, got %v", sample.Score)
		}

		// Solution must not exist after verify
		if _, err := os.Stat(environment.targetSolutionDirectory); !os.IsNotExist(err) {
			t.Errorf("expected solution directory to not exist after verify")
		}
	})

	t.Run("failing reward 0", func(t *testing.T) {
		testScript := "#!/bin/bash\n" +
			"mkdir -p \"${LOGS_DIRECTORY}/verifier\"\n" +
			"echo 'verification failure details'\n" +
			"echo 0 > \"${LOGS_DIRECTORY}/verifier/reward.txt\"\n"
		environment := newTaskFixtureEnvironment(t, "sample-task", testScript, "Test instruction")

		// 1. Run prepare
		prepareOutput, err := environment.run("prepare", "sample-task")
		if err != nil {
			t.Fatalf("prepare failed: %v\noutput: %s", err, string(prepareOutput))
		}

		// 2. Run verify
		verifyOutput, err := environment.run("verify", "sample-task")
		if err != nil {
			t.Fatalf("verify exited non-zero on reward 0: %v\noutput: %s", err, string(verifyOutput))
		}

		results := environment.readResults(t)
		if len(results.Samples) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(results.Samples))
		}
		sample := results.Samples[0]
		if sample.Passed == nil || *sample.Passed {
			t.Errorf("expected passed: false, got %v", sample.Passed)
		}
		if sample.Score == nil || *sample.Score != 0.0 {
			t.Errorf("expected score: 0.0, got %v", sample.Score)
		}
		if !strings.Contains(sample.Message, "verification failure details") {
			t.Errorf("expected message to contain 'verification failure details', got %q", sample.Message)
		}
	})
}

func TestRunTask_VerifyStaleRewardIgnored(t *testing.T) {
	// testScript writes nothing to verifier logs
	testScript := "#!/bin/bash\necho 'verifier produced no reward'\n"
	environment := newTaskFixtureEnvironment(t, "sample-task", testScript, "Test instruction")

	prepareOutput, err := environment.run("prepare", "sample-task")
	if err != nil {
		t.Fatalf("prepare failed: %v\noutput: %s", err, string(prepareOutput))
	}

	// Write stale reward files to simulate an agent leaving rewards behind
	verifierLogsDirectory := filepath.Join(environment.logsDirectory, "verifier")
	if err := os.MkdirAll(verifierLogsDirectory, 0755); err != nil {
		t.Fatalf("failed to create verifier log directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(verifierLogsDirectory, "reward.txt"), []byte("1\n"), 0644); err != nil {
		t.Fatalf("failed to write stale reward.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(verifierLogsDirectory, "reward.json"), []byte("{\"reward\": 1.0}\n"), 0644); err != nil {
		t.Fatalf("failed to write stale reward.json: %v", err)
	}

	// Verify must remove the stale rewards and fail because testScript writes nothing
	verifyOutput, err := environment.run("verify", "sample-task")
	if err != nil {
		t.Fatalf("verify failed unexpectedly: %v\noutput: %s", err, string(verifyOutput))
	}

	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "verifier did not write reward file") {
		t.Errorf("expected message to mention 'verifier did not write reward file', got %q", sample.Message)
	}
}

func TestRunTask_VerifyWithoutPrepareFailsClearly(t *testing.T) {
	environment := newTaskFixtureEnvironment(t, "sample-task", "#!/bin/bash\necho 1\n", "Test instruction")

	// Call verify directly without running prepare
	output, err := environment.run("verify", "sample-task")
	if err == nil {
		t.Fatalf("expected verify without prepare to fail, but succeeded\noutput: %s", string(output))
	}

	if !strings.Contains(string(output), "prepare") {
		t.Errorf("expected output to mention 'prepare', got: %s", string(output))
	}

	results := environment.readResults(t)
	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "prepare") {
		t.Errorf("expected message to mention 'prepare', got %q", sample.Message)
	}
}

func TestRunTask_FetchFailureNoTools(t *testing.T) {
	temporaryDirectory := t.TempDir()

	restrictedBinDirectory := filepath.Join(temporaryDirectory, "bin")
	if err := os.MkdirAll(restrictedBinDirectory, 0755); err != nil {
		t.Fatalf("failed to create bin directory: %v", err)
	}

	// Symlink only essential shell utilities, intentionally excluding git, curl, wget, python3, apt-get, apk
	essentialUtilities := []string{"bash", "sh", "awk", "cat", "rm", "mkdir", "tail", "grep", "sed", "chmod", "printf", "echo", "test"}
	for _, utility := range essentialUtilities {
		path, err := exec.LookPath(utility)
		if err == nil {
			_ = os.Symlink(path, filepath.Join(restrictedBinDirectory, utility))
		}
	}

	logsDirectory := filepath.Join(temporaryDirectory, "logs")
	outputDirectory := filepath.Join(temporaryDirectory, "out")
	targetSolutionDirectory := filepath.Join(temporaryDirectory, "solution")
	targetTestsDirectory := filepath.Join(temporaryDirectory, "tests")

	runTaskScript, err := filepath.Abs("run-task.sh")
	if err != nil {
		t.Fatalf("failed to resolve run-task.sh path: %v", err)
	}

	command := exec.Command(filepath.Join(restrictedBinDirectory, "bash"), runTaskScript, "sample-task", "oracle", "/app")
	command.Dir = temporaryDirectory
	command.Env = []string{
		"PATH=" + restrictedBinDirectory,
		"PINNED_COMMIT=fake-commit",
		"LOGS_DIRECTORY=" + logsDirectory,
		"OUT_DIRECTORY=" + outputDirectory,
		"SOLUTION_DIRECTORY=" + targetSolutionDirectory,
		"TESTS_DIRECTORY=" + targetTestsDirectory,
	}

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run-task.sh exited with non-zero status: %v\noutput: %s", err, string(output))
	}

	resultsPath := filepath.Join(outputDirectory, "results.json")
	resultsBytes, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("expected results.json to exist after fetch failure, but read failed: %v", err)
	}

	var results artifacts.ResultsFile
	if err := json.Unmarshal(resultsBytes, &results); err != nil {
		t.Fatalf("failed to parse results.json: %v\ncontent: %s", err, string(resultsBytes))
	}

	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample in results.json, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "failed to fetch repository") {
		t.Errorf("expected message to mention 'failed to fetch repository', got %q", sample.Message)
	}
}

func TestRunTask_PinnedCommitMissingFails(t *testing.T) {
	temporaryDirectory := t.TempDir()
	logsDirectory := filepath.Join(temporaryDirectory, "logs")
	outputDirectory := filepath.Join(temporaryDirectory, "out")
	targetSolutionDirectory := filepath.Join(temporaryDirectory, "solution")
	targetTestsDirectory := filepath.Join(temporaryDirectory, "tests")

	// Copy run-task.sh to temporaryDirectory without version.sh so PINNED_COMMIT cannot be resolved
	runTaskSourcePath, err := filepath.Abs("run-task.sh")
	if err != nil {
		t.Fatalf("failed to resolve run-task.sh path: %v", err)
	}
	runTaskScript := filepath.Join(temporaryDirectory, "run-task.sh")
	scriptContent, err := os.ReadFile(runTaskSourcePath)
	if err != nil {
		t.Fatalf("failed to read run-task.sh: %v", err)
	}
	if err := os.WriteFile(runTaskScript, scriptContent, 0755); err != nil {
		t.Fatalf("failed to copy run-task.sh: %v", err)
	}

	command := exec.Command("bash", runTaskScript, "sample-task", "oracle", "/app")
	command.Dir = temporaryDirectory
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"LOGS_DIRECTORY=" + logsDirectory,
		"OUT_DIRECTORY=" + outputDirectory,
		"SOLUTION_DIRECTORY=" + targetSolutionDirectory,
		"TESTS_DIRECTORY=" + targetTestsDirectory,
	}

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run-task.sh exited with non-zero status: %v\noutput: %s", err, string(output))
	}

	resultsPath := filepath.Join(outputDirectory, "results.json")
	resultsBytes, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("expected results.json to exist, but read failed: %v", err)
	}

	var results artifacts.ResultsFile
	if err := json.Unmarshal(resultsBytes, &results); err != nil {
		t.Fatalf("failed to parse results.json: %v\ncontent: %s", err, string(resultsBytes))
	}

	if len(results.Samples) != 1 {
		t.Fatalf("expected 1 sample in results.json, got %d", len(results.Samples))
	}
	sample := results.Samples[0]
	if sample.Passed == nil || *sample.Passed {
		t.Errorf("expected passed: false, got %v", sample.Passed)
	}
	if !strings.Contains(sample.Message, "PINNED_COMMIT not set: version.sh not found") {
		t.Errorf("expected message to mention 'PINNED_COMMIT not set: version.sh not found', got %q", sample.Message)
	}
}
