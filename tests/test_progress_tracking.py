"""
Tests for progress tracking functionality.

This module tests the progress tracking system and ensures it correctly
tracks student progress and provides appropriate feedback.
"""

from scripts.validate_exercises import ExerciseValidationResult
from scripts.progress_tracker import ProgressTracker, ProgressEntry, StudentProgress
import os
import sys
import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock
from datetime import datetime

# Import the progress tracker
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestProgressEntry:
    """Test the ProgressEntry dataclass."""

    def test_progress_entry_creation(self):
        """Test creating progress entries."""
        entry = ProgressEntry(
            timestamp="2024-01-01T10:00:00",
            module="01-setup",
            exercise_id="exercise-1",
            status="completed",
            score=0.9,
            time_spent=25,
            attempts=1
        )

        assert entry.timestamp == "2024-01-01T10:00:00"
        assert entry.module == "01-setup"
        assert entry.exercise_id == "exercise-1"
        assert entry.status == "completed"
        assert entry.score == 0.9
        assert entry.time_spent == 25
        assert entry.attempts == 1
        assert entry.feedback_shown is False  # Default value


class TestStudentProgress:
    """Test the StudentProgress dataclass."""

    def test_student_progress_creation(self):
        """Test creating student progress."""
        progress = StudentProgress(
            student_id="test123",
            start_date="2024-01-01T09:00:00",
            last_activity="2024-01-01T10:00:00",
            total_time_spent=60,
            modules_completed=["01-setup"],
            current_module="02-dag-fundamentals",
            current_exercise="exercise-1",
            progress_entries=[],
            achievements=["first_dag"]
        )

        assert progress.student_id == "test123"
        assert progress.start_date == "2024-01-01T09:00:00"
        assert progress.total_time_spent == 60
        assert "01-setup" in progress.modules_completed
        assert "first_dag" in progress.achievements


class TestProgressTracker:
    """Test the ProgressTracker class."""

    def test_tracker_initialization(self, tmp_path):
        """Test progress tracker initialization."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        assert tracker.progress_file == str(progress_file)
        assert isinstance(tracker.progress_data, StudentProgress)
        assert isinstance(tracker.feedback_rules, dict)

    def test_generate_student_id(self, tmp_path):
        """Test student ID generation."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        student_id = tracker._generate_student_id()
        assert isinstance(student_id, str)
        assert len(student_id) == 12  # MD5 hash truncated to 12 chars

    def test_create_new_progress(self, tmp_path):
        """Test creating new progress."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        progress = tracker._create_new_progress()

        assert isinstance(progress, StudentProgress)
        assert progress.current_module == '01-setup'
        assert progress.current_exercise == 'exercise-1'
        assert progress.total_time_spent == 0
        assert len(progress.progress_entries) == 0

    def test_save_and_load_progress(self, tmp_path):
        """Test saving and loading progress."""
        progress_file = tmp_path / "test_progress.json"

        # Create tracker and modify progress
        tracker = ProgressTracker(str(progress_file))
        tracker.progress_data.total_time_spent = 120
        tracker.progress_data.achievements.append("test_achievement")
        tracker._save_progress()

        # Create new tracker and verify data loaded
        tracker2 = ProgressTracker(str(progress_file))
        assert tracker2.progress_data.total_time_spent == 120
        assert "test_achievement" in tracker2.progress_data.achievements

    def test_load_progress_invalid_file(self, tmp_path):
        """Test loading progress with invalid JSON file."""
        progress_file = tmp_path / "invalid.json"
        progress_file.write_text("invalid json content")

        tracker = ProgressTracker(str(progress_file))

        # Should create new progress when file is invalid
        assert isinstance(tracker.progress_data, StudentProgress)
        assert tracker.progress_data.total_time_spent == 0

    def test_record_exercise_attempt_completed(self, tmp_path):
        """Test recording a completed exercise attempt."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Create mock validation result
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=0.9,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=["Great job!"],
            errors=[]
        )

        initial_entries = len(tracker.progress_data.progress_entries)
        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 30)

        # Check that entry was added
        assert len(tracker.progress_data.progress_entries) == initial_entries + 1

        latest_entry = tracker.progress_data.progress_entries[-1]
        assert latest_entry.module == "01-setup"
        assert latest_entry.exercise_id == "exercise-1"
        assert latest_entry.status == "completed"
        assert latest_entry.score == 0.9
        assert latest_entry.time_spent == 30
        assert latest_entry.attempts == 1

    def test_record_exercise_attempt_failed(self, tmp_path):
        """Test recording a failed exercise attempt."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Create mock validation result for failure
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=False,
            score=0.0,
            requirements_met=[],
            requirements_failed=["Missing DAG"],
            feedback=["Keep trying!"],
            errors=["DAG not found"]
        )

        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 15)

        latest_entry = tracker.progress_data.progress_entries[-1]
        assert latest_entry.status == "failed"
        assert latest_entry.score == 0.0

    def test_record_exercise_attempt_in_progress(self, tmp_path):
        """Test recording an in-progress exercise attempt."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Create mock validation result for partial completion
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=False,
            score=0.5,
            requirements_met=["DAG exists"],
            requirements_failed=["Missing tasks"],
            feedback=["Good start!"],
            errors=[]
        )

        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 20)

        latest_entry = tracker.progress_data.progress_entries[-1]
        assert latest_entry.status == "in_progress"
        assert latest_entry.score == 0.5

    def test_multiple_attempts_counting(self, tmp_path):
        """Test that multiple attempts are counted correctly."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=False,
            score=0.3,
            requirements_met=[],
            requirements_failed=["Multiple issues"],
            feedback=[],
            errors=[]
        )

        # Record multiple attempts
        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 10)
        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 15)
        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 20)

        # Check attempt counts
        entries = [e for e in tracker.progress_data.progress_entries
                   if e.module == "01-setup" and e.exercise_id == "exercise-1"]

        assert len(entries) == 3
        assert entries[0].attempts == 1
        assert entries[1].attempts == 2
        assert entries[2].attempts == 3

    def test_check_achievements_first_dag(self, tmp_path):
        """Test first DAG achievement."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=1.0,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 25)

        assert "first_dag" in tracker.progress_data.achievements

    def test_check_achievements_speed_learner(self, tmp_path):
        """Test speed learner achievement."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="02-dag-fundamentals",
            passed=True,
            score=1.0,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        # Complete exercise in under 15 minutes
        tracker.record_exercise_attempt(
            "02-dag-fundamentals", "exercise-1", result, 10)

        assert "speed_learner" in tracker.progress_data.achievements

    def test_check_achievements_persistent_learner(self, tmp_path):
        """Test persistent learner achievement."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Create failed attempts first
        failed_result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=False,
            score=0.3,
            requirements_met=[],
            requirements_failed=["Issues"],
            feedback=[],
            errors=[]
        )

        # Record multiple failed attempts
        tracker.record_exercise_attempt(
            "01-setup", "exercise-1", failed_result, 15)
        tracker.record_exercise_attempt(
            "01-setup", "exercise-1", failed_result, 20)

        # Finally succeed on 3rd attempt
        success_result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=1.0,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        tracker.record_exercise_attempt(
            "01-setup", "exercise-1", success_result, 25)

        assert "persistent_learner" in tracker.progress_data.achievements

    def test_get_personalized_feedback_success(self, tmp_path):
        """Test personalized feedback for successful completion."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=1.0,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        feedback = tracker.get_personalized_feedback(result)

        assert len(feedback) > 0
        # Should contain encouragement
        encouragement_found = any(
            "🚀" in fb or "💪" in fb or "🎯" in fb or "⭐" in fb or "🔥" in fb for fb in feedback)
        assert encouragement_found

    def test_get_personalized_feedback_with_errors(self, tmp_path):
        """Test personalized feedback with common errors."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=False,
            score=0.2,
            requirements_met=[],
            requirements_failed=["Missing DAG import"],
            feedback=[],
            errors=["No DAG import found in file"]
        )

        feedback = tracker.get_personalized_feedback(result)

        assert len(feedback) > 0
        # Should contain specific feedback for missing import
        import_feedback_found = any("import DAG" in fb for fb in feedback)
        assert import_feedback_found

    def test_get_progress_summary(self, tmp_path):
        """Test progress summary generation."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Add some progress entries
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=0.9,
            requirements_met=["Requirements met"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 30)

        summary = tracker.get_progress_summary()

        assert "student_id" in summary
        assert "start_date" in summary
        assert "total_time_spent" in summary
        assert "completion_rate" in summary
        assert "average_score" in summary
        assert "current_module" in summary
        assert "achievements" in summary

        assert summary["total_attempts"] == 1
        assert summary["completed_exercises"] == 1
        assert summary["completion_rate"] == 1.0
        assert summary["average_score"] == 0.9

    def test_get_next_recommendation_continue(self, tmp_path):
        """Test next recommendation when current exercise is completed."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Complete current exercise
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=1.0,
            requirements_met=["All requirements"],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        tracker.record_exercise_attempt("01-setup", "exercise-1", result, 25)

        recommendation = tracker.get_next_recommendation()

        assert recommendation["type"] == "continue"
        assert "Great job" in recommendation["message"]

    def test_get_next_recommendation_retry(self, tmp_path):
        """Test next recommendation when current exercise is not completed."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        recommendation = tracker.get_next_recommendation()

        assert recommendation["type"] == "retry"
        assert "Keep working" in recommendation["message"]

    def test_show_achievements_none(self, tmp_path, capsys):
        """Test showing achievements when none are earned."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        tracker.show_achievements()

        captured = capsys.readouterr()
        assert "No achievements yet" in captured.out

    def test_show_achievements_with_achievements(self, tmp_path, capsys):
        """Test showing achievements when some are earned."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Add an achievement
        tracker.progress_data.achievements.append("first_dag")

        tracker.show_achievements()

        captured = capsys.readouterr()
        assert "YOUR ACHIEVEMENTS" in captured.out
        assert "First DAG Creator" in captured.out

    def test_reset_progress(self, tmp_path):
        """Test resetting progress."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        # Add some progress
        tracker.progress_data.total_time_spent = 120
        tracker.progress_data.achievements.append("test")
        tracker._save_progress()

        # Reset progress
        tracker.reset_progress()

        # Verify reset
        assert tracker.progress_data.total_time_spent == 0
        assert len(tracker.progress_data.achievements) == 0
        assert not os.path.exists(progress_file)

    def test_load_feedback_rules(self, tmp_path):
        """Test loading feedback rules."""
        progress_file = tmp_path / "test_progress.json"
        tracker = ProgressTracker(str(progress_file))

        rules = tracker.feedback_rules

        assert "common_mistakes" in rules
        assert "encouragement" in rules
        assert "achievements" in rules

        # Check common mistakes structure
        mistakes = rules["common_mistakes"]
        assert "missing_dag_import" in mistakes
        assert "pattern" in mistakes["missing_dag_import"]
        assert "feedback" in mistakes["missing_dag_import"]
        assert "hint" in mistakes["missing_dag_import"]


class TestProgressTrackerScript:
    """Test the progress tracker script functionality."""

    def test_main_function_exists(self):
        """Test that the main progress tracker script can be imported."""
        try:
            from scripts.progress_tracker import main
            assert callable(main)
        except ImportError:
            pytest.fail(
                "Cannot import main function from progress_tracker script")

    @patch('sys.argv', ['progress_tracker.py', '--summary'])
    def test_main_function_with_summary(self, tmp_path):
        """Test main function with summary flag."""
        with patch('scripts.progress_tracker.ProgressTracker') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker.get_progress_summary.return_value = {
                'student_id': 'test123',
                'start_date': '2024-01-01T09:00:00',
                'days_active': 1,
                'total_time_spent': 60,
                'avg_time_per_day': 60.0,
                'total_attempts': 1,
                'completed_exercises': 1,
                'completion_rate': 1.0,
                'average_score': 0.9,
                'current_module': '01-setup',
                'current_exercise': 'exercise-1',
                'modules_completed': [],
                'achievements': ['first_dag']
            }
            mock_tracker_class.return_value = mock_tracker

            from scripts.progress_tracker import main

            try:
                main()
            except SystemExit:
                pass  # Expected for command line scripts

    @patch('sys.argv', ['progress_tracker.py', '--achievements'])
    def test_main_function_with_achievements(self):
        """Test main function with achievements flag."""
        with patch('scripts.progress_tracker.ProgressTracker') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker_class.return_value = mock_tracker

            from scripts.progress_tracker import main

            try:
                main()
            except SystemExit:
                pass  # Expected for command line scripts
