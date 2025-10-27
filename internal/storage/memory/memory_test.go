package memory

import (
	"context"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func setupTestMemory(t *testing.T) *MemoryStorage {
	t.Helper()

	store := New("")
	ctx := context.Background()

	// Set issue_prefix config
	if err := store.SetConfig(ctx, "issue_prefix", "bd"); err != nil {
		t.Fatalf("failed to set issue_prefix: %v", err)
	}

	return store
}

func TestCreateIssue(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()
	issue := &types.Issue{
		Title:       "Test issue",
		Description: "Test description",
		Status:      types.StatusOpen,
		Priority:    1,
		IssueType:   types.TypeTask,
	}

	err := store.CreateIssue(ctx, issue, "test-user")
	if err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	if issue.ID == "" {
		t.Error("Issue ID should be set")
	}

	if !issue.CreatedAt.After(time.Time{}) {
		t.Error("CreatedAt should be set")
	}

	if !issue.UpdatedAt.After(time.Time{}) {
		t.Error("UpdatedAt should be set")
	}
}

func TestCreateIssueValidation(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		issue   *types.Issue
		wantErr bool
	}{
		{
			name: "valid issue",
			issue: &types.Issue{
				Title:     "Valid",
				Status:    types.StatusOpen,
				Priority:  2,
				IssueType: types.TypeTask,
			},
			wantErr: false,
		},
		{
			name: "missing title",
			issue: &types.Issue{
				Status:    types.StatusOpen,
				Priority:  2,
				IssueType: types.TypeTask,
			},
			wantErr: true,
		},
		{
			name: "invalid priority",
			issue: &types.Issue{
				Title:     "Test",
				Status:    types.StatusOpen,
				Priority:  10,
				IssueType: types.TypeTask,
			},
			wantErr: true,
		},
		{
			name: "invalid status",
			issue: &types.Issue{
				Title:     "Test",
				Status:    "invalid",
				Priority:  2,
				IssueType: types.TypeTask,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.CreateIssue(ctx, tt.issue, "test-user")
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateIssue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetIssue(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()
	original := &types.Issue{
		Title:              "Test issue",
		Description:        "Description",
		Design:             "Design notes",
		AcceptanceCriteria: "Acceptance",
		Notes:              "Notes",
		Status:             types.StatusOpen,
		Priority:           1,
		IssueType:          types.TypeFeature,
		Assignee:           "alice",
	}

	err := store.CreateIssue(ctx, original, "test-user")
	if err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Retrieve the issue
	retrieved, err := store.GetIssue(ctx, original.ID)
	if err != nil {
		t.Fatalf("GetIssue failed: %v", err)
	}

	if retrieved == nil {
		t.Fatal("GetIssue returned nil")
	}

	if retrieved.ID != original.ID {
		t.Errorf("ID mismatch: got %v, want %v", retrieved.ID, original.ID)
	}

	if retrieved.Title != original.Title {
		t.Errorf("Title mismatch: got %v, want %v", retrieved.Title, original.Title)
	}

	if retrieved.Description != original.Description {
		t.Errorf("Description mismatch: got %v, want %v", retrieved.Description, original.Description)
	}

	if retrieved.Assignee != original.Assignee {
		t.Errorf("Assignee mismatch: got %v, want %v", retrieved.Assignee, original.Assignee)
	}
}

func TestGetIssueNotFound(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()
	issue, err := store.GetIssue(ctx, "bd-999")
	if err != nil {
		t.Fatalf("GetIssue failed: %v", err)
	}

	if issue != nil {
		t.Errorf("Expected nil for non-existent issue, got %v", issue)
	}
}

func TestCreateIssues(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		issues  []*types.Issue
		wantErr bool
	}{
		{
			name:    "empty batch",
			issues:  []*types.Issue{},
			wantErr: false,
		},
		{
			name: "single issue",
			issues: []*types.Issue{
				{Title: "Single issue", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
			},
			wantErr: false,
		},
		{
			name: "multiple issues",
			issues: []*types.Issue{
				{Title: "Issue 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
				{Title: "Issue 2", Status: types.StatusInProgress, Priority: 2, IssueType: types.TypeBug},
				{Title: "Issue 3", Status: types.StatusOpen, Priority: 3, IssueType: types.TypeFeature},
			},
			wantErr: false,
		},
		{
			name: "validation error - missing title",
			issues: []*types.Issue{
				{Title: "Valid issue", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
				{Title: "", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
			},
			wantErr: true,
		},
		{
			name: "duplicate ID within batch error",
			issues: []*types.Issue{
				{ID: "dup-1", Title: "First", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
				{ID: "dup-1", Title: "Second", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh storage for each test
			testStore := setupTestMemory(t)
			defer testStore.Close()

			err := testStore.CreateIssues(ctx, tt.issues, "test-user")
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateIssues() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && len(tt.issues) > 0 {
				// Verify all issues got IDs
				for i, issue := range tt.issues {
					if issue.ID == "" {
						t.Errorf("issue %d: ID should be set", i)
					}
					if !issue.CreatedAt.After(time.Time{}) {
						t.Errorf("issue %d: CreatedAt should be set", i)
					}
				}
			}
		})
	}
}

func TestUpdateIssue(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create an issue
	issue := &types.Issue{
		Title:     "Original",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Update it
	updates := map[string]interface{}{
		"title":    "Updated",
		"priority": 1,
		"status":   string(types.StatusInProgress),
	}
	if err := store.UpdateIssue(ctx, issue.ID, updates, "test-user"); err != nil {
		t.Fatalf("UpdateIssue failed: %v", err)
	}

	// Retrieve and verify
	updated, err := store.GetIssue(ctx, issue.ID)
	if err != nil {
		t.Fatalf("GetIssue failed: %v", err)
	}

	if updated.Title != "Updated" {
		t.Errorf("Title not updated: got %v", updated.Title)
	}

	if updated.Priority != 1 {
		t.Errorf("Priority not updated: got %v", updated.Priority)
	}

	if updated.Status != types.StatusInProgress {
		t.Errorf("Status not updated: got %v", updated.Status)
	}
}

func TestCloseIssue(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create an issue
	issue := &types.Issue{
		Title:     "Test",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Close it
	if err := store.CloseIssue(ctx, issue.ID, "Completed", "test-user"); err != nil {
		t.Fatalf("CloseIssue failed: %v", err)
	}

	// Verify
	closed, err := store.GetIssue(ctx, issue.ID)
	if err != nil {
		t.Fatalf("GetIssue failed: %v", err)
	}

	if closed.Status != types.StatusClosed {
		t.Errorf("Status should be closed, got %v", closed.Status)
	}

	if closed.ClosedAt == nil {
		t.Error("ClosedAt should be set")
	}
}

func TestSearchIssues(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create test issues
	issues := []*types.Issue{
		{Title: "Bug fix", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeBug},
		{Title: "New feature", Status: types.StatusInProgress, Priority: 2, IssueType: types.TypeFeature},
		{Title: "Task", Status: types.StatusOpen, Priority: 3, IssueType: types.TypeTask},
	}

	for _, issue := range issues {
		if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
			t.Fatalf("CreateIssue failed: %v", err)
		}
	}

	tests := []struct {
		name     string
		query    string
		filter   types.IssueFilter
		wantSize int
	}{
		{
			name:     "all issues",
			query:    "",
			filter:   types.IssueFilter{},
			wantSize: 3,
		},
		{
			name:     "search by title",
			query:    "feature",
			filter:   types.IssueFilter{},
			wantSize: 1,
		},
		{
			name:     "filter by status",
			query:    "",
			filter:   types.IssueFilter{Status: func() *types.Status { s := types.StatusOpen; return &s }()},
			wantSize: 2,
		},
		{
			name:     "filter by priority",
			query:    "",
			filter:   types.IssueFilter{Priority: func() *int { p := 1; return &p }()},
			wantSize: 1,
		},
		{
			name:     "filter by type",
			query:    "",
			filter:   types.IssueFilter{IssueType: func() *types.IssueType { t := types.TypeBug; return &t }()},
			wantSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := store.SearchIssues(ctx, tt.query, tt.filter)
			if err != nil {
				t.Fatalf("SearchIssues failed: %v", err)
			}

			if len(results) != tt.wantSize {
				t.Errorf("Expected %d results, got %d", tt.wantSize, len(results))
			}
		})
	}
}

func TestDependencies(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create two issues
	issue1 := &types.Issue{
		Title:     "Issue 1",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}
	issue2 := &types.Issue{
		Title:     "Issue 2",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}

	if err := store.CreateIssue(ctx, issue1, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}
	if err := store.CreateIssue(ctx, issue2, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Add dependency
	dep := &types.Dependency{
		IssueID:     issue1.ID,
		DependsOnID: issue2.ID,
		Type:        types.DepBlocks,
	}
	if err := store.AddDependency(ctx, dep, "test-user"); err != nil {
		t.Fatalf("AddDependency failed: %v", err)
	}

	// Get dependencies
	deps, err := store.GetDependencies(ctx, issue1.ID)
	if err != nil {
		t.Fatalf("GetDependencies failed: %v", err)
	}

	if len(deps) != 1 {
		t.Errorf("Expected 1 dependency, got %d", len(deps))
	}

	if deps[0].ID != issue2.ID {
		t.Errorf("Dependency mismatch: got %v", deps[0].ID)
	}

	// Get dependents
	dependents, err := store.GetDependents(ctx, issue2.ID)
	if err != nil {
		t.Fatalf("GetDependents failed: %v", err)
	}

	if len(dependents) != 1 {
		t.Errorf("Expected 1 dependent, got %d", len(dependents))
	}

	// Remove dependency
	if err := store.RemoveDependency(ctx, issue1.ID, issue2.ID, "test-user"); err != nil {
		t.Fatalf("RemoveDependency failed: %v", err)
	}

	// Verify removed
	deps, err = store.GetDependencies(ctx, issue1.ID)
	if err != nil {
		t.Fatalf("GetDependencies failed: %v", err)
	}

	if len(deps) != 0 {
		t.Errorf("Expected 0 dependencies after removal, got %d", len(deps))
	}
}

func TestLabels(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create an issue
	issue := &types.Issue{
		Title:     "Test",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Add labels
	if err := store.AddLabel(ctx, issue.ID, "bug", "test-user"); err != nil {
		t.Fatalf("AddLabel failed: %v", err)
	}
	if err := store.AddLabel(ctx, issue.ID, "critical", "test-user"); err != nil {
		t.Fatalf("AddLabel failed: %v", err)
	}

	// Get labels
	labels, err := store.GetLabels(ctx, issue.ID)
	if err != nil {
		t.Fatalf("GetLabels failed: %v", err)
	}

	if len(labels) != 2 {
		t.Errorf("Expected 2 labels, got %d", len(labels))
	}

	// Remove label
	if err := store.RemoveLabel(ctx, issue.ID, "bug", "test-user"); err != nil {
		t.Fatalf("RemoveLabel failed: %v", err)
	}

	// Verify
	labels, err = store.GetLabels(ctx, issue.ID)
	if err != nil {
		t.Fatalf("GetLabels failed: %v", err)
	}

	if len(labels) != 1 {
		t.Errorf("Expected 1 label after removal, got %d", len(labels))
	}
}

func TestComments(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create an issue
	issue := &types.Issue{
		Title:     "Test",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Add comment
	comment, err := store.AddIssueComment(ctx, issue.ID, "alice", "First comment")
	if err != nil {
		t.Fatalf("AddIssueComment failed: %v", err)
	}

	if comment == nil {
		t.Fatal("Comment should not be nil")
	}

	// Get comments
	comments, err := store.GetIssueComments(ctx, issue.ID)
	if err != nil {
		t.Fatalf("GetIssueComments failed: %v", err)
	}

	if len(comments) != 1 {
		t.Errorf("Expected 1 comment, got %d", len(comments))
	}

	if comments[0].Text != "First comment" {
		t.Errorf("Comment text mismatch: got %v", comments[0].Text)
	}
}

func TestLoadFromIssues(t *testing.T) {
	store := New("")
	defer store.Close()

	issues := []*types.Issue{
		{
			ID:           "bd-1",
			Title:        "Issue 1",
			Status:       types.StatusOpen,
			Priority:     1,
			IssueType:    types.TypeTask,
			Labels:       []string{"bug", "critical"},
			Dependencies: []*types.Dependency{{IssueID: "bd-1", DependsOnID: "bd-2", Type: types.DepBlocks}},
		},
		{
			ID:        "bd-2",
			Title:     "Issue 2",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
		},
	}

	if err := store.LoadFromIssues(issues); err != nil {
		t.Fatalf("LoadFromIssues failed: %v", err)
	}

	// Verify issues loaded
	ctx := context.Background()
	loaded, err := store.GetIssue(ctx, "bd-1")
	if err != nil {
		t.Fatalf("GetIssue failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("Issue should be loaded")
	}

	if loaded.Title != "Issue 1" {
		t.Errorf("Title mismatch: got %v", loaded.Title)
	}

	// Verify labels loaded
	if len(loaded.Labels) != 2 {
		t.Errorf("Expected 2 labels, got %d", len(loaded.Labels))
	}

	// Verify dependencies loaded
	if len(loaded.Dependencies) != 1 {
		t.Errorf("Expected 1 dependency, got %d", len(loaded.Dependencies))
	}

	// Verify counter updated
	if store.counters["bd"] != 2 {
		t.Errorf("Expected counter bd=2, got %d", store.counters["bd"])
	}
}

func TestGetAllIssues(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create issues
	for i := 1; i <= 3; i++ {
		issue := &types.Issue{
			Title:     "Issue",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
		}
		if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
			t.Fatalf("CreateIssue failed: %v", err)
		}
	}

	// Get all
	all := store.GetAllIssues()
	if len(all) != 3 {
		t.Errorf("Expected 3 issues, got %d", len(all))
	}

	// Verify sorted by ID
	for i := 1; i < len(all); i++ {
		if all[i-1].ID >= all[i].ID {
			t.Error("Issues should be sorted by ID")
		}
	}
}

func TestDirtyTracking(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create an issue
	issue := &types.Issue{
		Title:     "Test",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
		t.Fatalf("CreateIssue failed: %v", err)
	}

	// Should be dirty
	dirty, err := store.GetDirtyIssues(ctx)
	if err != nil {
		t.Fatalf("GetDirtyIssues failed: %v", err)
	}

	if len(dirty) != 1 {
		t.Errorf("Expected 1 dirty issue, got %d", len(dirty))
	}

	// Clear dirty
	if err := store.ClearDirtyIssues(ctx); err != nil {
		t.Fatalf("ClearDirtyIssues failed: %v", err)
	}

	dirty, err = store.GetDirtyIssues(ctx)
	if err != nil {
		t.Fatalf("GetDirtyIssues failed: %v", err)
	}

	if len(dirty) != 0 {
		t.Errorf("Expected 0 dirty issues after clear, got %d", len(dirty))
	}
}

func TestStatistics(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Create issues with different statuses
	issues := []*types.Issue{
		{Title: "Open 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
		{Title: "Open 2", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
		{Title: "In Progress", Status: types.StatusInProgress, Priority: 1, IssueType: types.TypeTask},
		{Title: "Closed", Status: types.StatusClosed, Priority: 1, IssueType: types.TypeTask, ClosedAt: func() *time.Time { t := time.Now(); return &t }()},
	}

	for _, issue := range issues {
		if err := store.CreateIssue(ctx, issue, "test-user"); err != nil {
			t.Fatalf("CreateIssue failed: %v", err)
		}
		// Close the one marked as closed
		if issue.Status == types.StatusClosed {
			if err := store.CloseIssue(ctx, issue.ID, "Done", "test-user"); err != nil {
				t.Fatalf("CloseIssue failed: %v", err)
			}
		}
	}

	stats, err := store.GetStatistics(ctx)
	if err != nil {
		t.Fatalf("GetStatistics failed: %v", err)
	}

	if stats.TotalIssues != 4 {
		t.Errorf("Expected 4 total issues, got %d", stats.TotalIssues)
	}
	if stats.OpenIssues != 2 {
		t.Errorf("Expected 2 open issues, got %d", stats.OpenIssues)
	}
	if stats.InProgressIssues != 1 {
		t.Errorf("Expected 1 in-progress issue, got %d", stats.InProgressIssues)
	}
	if stats.ClosedIssues != 1 {
		t.Errorf("Expected 1 closed issue, got %d", stats.ClosedIssues)
	}
}

func TestConfigOperations(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Set config
	if err := store.SetConfig(ctx, "test_key", "test_value"); err != nil {
		t.Fatalf("SetConfig failed: %v", err)
	}

	// Get config
	value, err := store.GetConfig(ctx, "test_key")
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}

	if value != "test_value" {
		t.Errorf("Expected test_value, got %v", value)
	}

	// Get all config
	allConfig, err := store.GetAllConfig(ctx)
	if err != nil {
		t.Fatalf("GetAllConfig failed: %v", err)
	}

	if len(allConfig) < 1 {
		t.Error("Expected at least 1 config entry")
	}

	// Delete config
	if err := store.DeleteConfig(ctx, "test_key"); err != nil {
		t.Fatalf("DeleteConfig failed: %v", err)
	}

	value, err = store.GetConfig(ctx, "test_key")
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}

	if value != "" {
		t.Errorf("Expected empty value after delete, got %v", value)
	}
}

func TestMetadataOperations(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()

	// Set metadata
	if err := store.SetMetadata(ctx, "hash", "abc123"); err != nil {
		t.Fatalf("SetMetadata failed: %v", err)
	}

	// Get metadata
	value, err := store.GetMetadata(ctx, "hash")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if value != "abc123" {
		t.Errorf("Expected abc123, got %v", value)
	}
}

func TestSyncAllCounters(t *testing.T) {
	store := New("")
	defer store.Close()

	ctx := context.Background()

	// Load issues with different prefixes
	issues := []*types.Issue{
		{ID: "bd-5", Title: "Test 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
		{ID: "bd-10", Title: "Test 2", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
		{ID: "custom-3", Title: "Test 3", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask},
	}

	if err := store.LoadFromIssues(issues); err != nil {
		t.Fatalf("LoadFromIssues failed: %v", err)
	}

	// Manually corrupt counter
	store.counters["bd"] = 1

	// Sync counters
	if err := store.SyncAllCounters(ctx); err != nil {
		t.Fatalf("SyncAllCounters failed: %v", err)
	}

	// Verify corrected
	if store.counters["bd"] != 10 {
		t.Errorf("Expected bd counter to be 10, got %d", store.counters["bd"])
	}

	if store.counters["custom"] != 3 {
		t.Errorf("Expected custom counter to be 3, got %d", store.counters["custom"])
	}
}

func TestThreadSafety(t *testing.T) {
	store := setupTestMemory(t)
	defer store.Close()

	ctx := context.Background()
	const numGoroutines = 10

	// Run concurrent creates
	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(n int) {
			issue := &types.Issue{
				Title:     "Concurrent",
				Status:    types.StatusOpen,
				Priority:  1,
				IssueType: types.TypeTask,
			}
			store.CreateIssue(ctx, issue, "test-user")
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all created
	stats, err := store.GetStatistics(ctx)
	if err != nil {
		t.Fatalf("GetStatistics failed: %v", err)
	}

	if stats.TotalIssues != numGoroutines {
		t.Errorf("Expected %d issues, got %d", numGoroutines, stats.TotalIssues)
	}
}

func TestClose(t *testing.T) {
	store := setupTestMemory(t)

	if store.closed {
		t.Error("Store should not be closed initially")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !store.closed {
		t.Error("Store should be closed")
	}
}
