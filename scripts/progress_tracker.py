#!/usr/bin/env python3
"""
Progress Tracking System

This script tracks student progress through the Airflow Coding Kata,
providing feedback on completion status and identifying common mistakes.

Usage:
    python scripts/progress_tracker.py
    python scripts/progress_tracker.py --save-progress
    python scripts/progress_tracker.py --show-feedback
"""

from validate_exercises import ExerciseValidator, ExerciseValidationResult
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import hashlib

# Import our exercise validator
sys.path.insert(0, str(Path(__file__).parent))


@dataclass
class ProgressEntry:
    """Represents a progress entry for a student."""
    timestamp: str
    module: str
    exercise_id: str
    status: str  # 'started', 'in_progress', 'completed', 'failed'
    score: float
    time_spent: int  # minutes
    attempts: int
    feedback_shown: bool = False


@dataclass
class StudentProgress:
    """Represents overall student progress."""
    student_id: str
    start_date: str
    last_activity: str
    total_time_spent: int  # minutes
    modules_completed: List[str]
    current_module: str
    current_exercise: str
    progress_entries: List[ProgressEntry]
    achievements: List[str]


class ProgressTracker:
    """Tracks and manages student progress."""

    def __init__(self, progress_file: str = ".kata_progress.json"):
        self.progress_file = progress_file
        self.validator = ExerciseValidator()
        self.progress_data = self._load_progress()
        self.feedback_rules = self._load_feedback_rules()

    def _generate_student_id(self) -> str:
        """Generate a unique student ID based on environment."""
        # Use a combination of username and working directory
        import getpass
        username = getpass.getuser()
        workdir = os.getcwd()
        combined = f"{username}:{workdir}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]

    def _load_progress(self) -> StudentProgress:
        """Load existing progress or create new progress tracking."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)

                # Convert progress entries back to dataclass objects
                progress_entries = [
                    ProgressEntry(**entry) for entry in data.get('progress_entries', [])
                ]

                return StudentProgress(
                    student_id=data.get(
                        'student_id', self._generate_student_id()),
                    start_date=data.get(
                        'start_date', datetime.now().isoformat()),
                    last_activity=data.get(
                        'last_activity', datetime.now().isoformat()),
                    total_time_spent=data.get('total_time_spent', 0),
                    modules_completed=data.get('modules_completed', []),
                    current_module=data.get('current_module', '01-setup'),
                    current_exercise=data.get(
                        'current_exercise', 'exercise-1'),
                    progress_entries=progress_entries,
                    achievements=data.get('achievements', [])
                )
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Could not load progress file: {e}")
                return self._create_new_progress()
        else:
            return self._create_new_progress()

    def _create_new_progress(self) -> StudentProgress:
        """Create new progress tracking."""
        return StudentProgress(
            student_id=self._generate_student_id(),
            start_date=datetime.now().isoformat(),
            last_activity=datetime.now().isoformat(),
            total_time_spent=0,
            modules_completed=[],
            current_module='01-setup',
            current_exercise='exercise-1',
            progress_entries=[],
            achievements=[]
        )

    def _save_progress(self):
        """Save progress to file."""
        # Convert dataclass to dictionary
        data = asdict(self.progress_data)

        try:
            with open(self.progress_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save progress: {e}")

    def _load_feedback_rules(self) -> Dict[str, Any]:
        """Load feedback rules for common mistakes."""
        return {
            "common_mistakes": {
                "missing_dag_import": {
                    "pattern": "No DAG import found",
                    "feedback": "💡 Don't forget to import DAG: `from airflow import DAG`",
                    "hint": "The DAG class is the foundation of every Airflow workflow."
                },
                "missing_task_id": {
                    "pattern": "task_id",
                    "feedback": "💡 Every task needs a unique task_id parameter.",
                    "hint": "Task IDs should be descriptive and follow snake_case convention."
                },
                "syntax_error": {
                    "pattern": "Syntax error",
                    "feedback": "🔧 Check your Python syntax - missing parentheses, quotes, or indentation?",
                    "hint": "Use a code editor with syntax highlighting to catch these errors."
                },
                "missing_dependencies": {
                    "pattern": "dependencies",
                    "feedback": "🔗 Remember to define task dependencies using >> or set_downstream().",
                    "hint": "Dependencies control the order in which tasks execute."
                },
                "missing_schedule": {
                    "pattern": "schedule",
                    "feedback": "⏰ Don't forget to set a schedule_interval for your DAG.",
                    "hint": "Use timedelta objects or cron expressions for scheduling."
                }
            },
            "encouragement": [
                "🚀 Great progress! Keep going!",
                "💪 You're getting the hang of this!",
                "🎯 Nice work on completing that exercise!",
                "⭐ Excellent! You're mastering Airflow concepts!",
                "🔥 You're on fire! Keep up the momentum!"
            ],
            "achievements": {
                "first_dag": {
                    "condition": "completed_first_exercise",
                    "title": "🎉 First DAG Creator",
                    "description": "Created your first Airflow DAG!"
                },
                "module_master": {
                    "condition": "completed_module",
                    "title": "📚 Module Master",
                    "description": "Completed an entire module!"
                },
                "speed_learner": {
                    "condition": "fast_completion",
                    "title": "⚡ Speed Learner",
                    "description": "Completed an exercise in record time!"
                },
                "persistent_learner": {
                    "condition": "multiple_attempts",
                    "title": "🎯 Persistent Learner",
                    "description": "Kept trying until you got it right!"
                }
            }
        }

    def record_exercise_attempt(self, module: str, exercise_id: str,
                                result: ExerciseValidationResult, time_spent: int = 0):
        """Record an exercise attempt."""
        now = datetime.now().isoformat()

        # Determine status
        if result.passed:
            status = 'completed'
        elif result.score > 0:
            status = 'in_progress'
        else:
            status = 'failed'

        # Count previous attempts for this exercise
        attempts = len([
            entry for entry in self.progress_data.progress_entries
            if entry.module == module and entry.exercise_id == exercise_id
        ]) + 1

        # Create progress entry
        entry = ProgressEntry(
            timestamp=now,
            module=module,
            exercise_id=exercise_id,
            status=status,
            score=result.score,
            time_spent=time_spent,
            attempts=attempts
        )

        self.progress_data.progress_entries.append(entry)
        self.progress_data.last_activity = now
        self.progress_data.total_time_spent += time_spent

        # Update current position if completed
        if result.passed:
            self._update_current_position(module, exercise_id)
            self._check_achievements(module, exercise_id, entry)

        self._save_progress()

    def _update_current_position(self, completed_module: str, completed_exercise: str):
        """Update current module and exercise position."""
        # This is a simplified version - in practice, you'd have a more sophisticated
        # system for determining the next exercise/module

        module_order = [
            '01-setup', '02-dag-fundamentals', '03-tasks-operators',
            '04-scheduling-dependencies', '05-sensors-triggers',
            '06-data-passing-xcoms', '07-branching-conditionals',
            '08-error-handling', '09-advanced-patterns', '10-real-world-projects'
        ]

        # Mark module as completed if this was the last exercise
        if completed_module not in self.progress_data.modules_completed:
            # Simple heuristic: if we completed an exercise, mark module as in progress
            # In practice, you'd check if all exercises in the module are completed
            pass

        # Update current position (simplified logic)
        try:
            current_index = module_order.index(completed_module)
            if current_index < len(module_order) - 1:
                next_module = module_order[current_index + 1]
                self.progress_data.current_module = next_module
                self.progress_data.current_exercise = 'exercise-1'
        except ValueError:
            pass  # Module not in standard order

    def _check_achievements(self, module: str, exercise_id: str, entry: ProgressEntry):
        """Check and award achievements."""
        achievements = self.feedback_rules["achievements"]

        # First DAG achievement
        if (module == '01-setup' and exercise_id == 'exercise-1' and
            entry.status == 'completed' and
                "first_dag" not in self.progress_data.achievements):
            self.progress_data.achievements.append("first_dag")

        # Speed learner achievement (completed in under 15 minutes)
        if (entry.status == 'completed' and entry.time_spent < 15 and
                "speed_learner" not in self.progress_data.achievements):
            self.progress_data.achievements.append("speed_learner")

        # Persistent learner achievement (multiple attempts)
        if (entry.status == 'completed' and entry.attempts >= 3 and
                "persistent_learner" not in self.progress_data.achievements):
            self.progress_data.achievements.append("persistent_learner")

    def get_personalized_feedback(self, result: ExerciseValidationResult) -> List[str]:
        """Generate personalized feedback based on validation results."""
        feedback = []

        # Add encouragement if passed
        if result.passed:
            import random
            encouragement = random.choice(self.feedback_rules["encouragement"])
            feedback.append(encouragement)

        # Add specific feedback for common mistakes
        common_mistakes = self.feedback_rules["common_mistakes"]

        for error in result.errors:
            for mistake_type, mistake_info in common_mistakes.items():
                if mistake_info["pattern"].lower() in error.lower():
                    feedback.append(mistake_info["feedback"])
                    feedback.append(f"   {mistake_info['hint']}")
                    break

        # Add progress-based feedback
        total_exercises = len(self.progress_data.progress_entries)
        if total_exercises == 1:
            feedback.append(
                "🌟 Welcome to the Airflow Coding Kata! You're just getting started.")
        elif total_exercises % 5 == 0:
            feedback.append(
                f"🎯 Milestone reached! You've attempted {total_exercises} exercises.")

        return feedback

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get a summary of current progress."""
        completed_exercises = [
            entry for entry in self.progress_data.progress_entries
            if entry.status == 'completed'
        ]

        total_attempts = len(self.progress_data.progress_entries)
        completion_rate = len(completed_exercises) / \
            total_attempts if total_attempts > 0 else 0.0

        # Calculate average score
        if self.progress_data.progress_entries:
            avg_score = sum(entry.score for entry in self.progress_data.progress_entries) / \
                len(self.progress_data.progress_entries)
        else:
            avg_score = 0.0

        # Time analysis
        start_date = datetime.fromisoformat(self.progress_data.start_date)
        days_active = (datetime.now() - start_date).days + 1
        avg_time_per_day = self.progress_data.total_time_spent / \
            days_active if days_active > 0 else 0

        return {
            'student_id': self.progress_data.student_id,
            'start_date': self.progress_data.start_date,
            'days_active': days_active,
            'total_time_spent': self.progress_data.total_time_spent,
            'avg_time_per_day': avg_time_per_day,
            'total_attempts': total_attempts,
            'completed_exercises': len(completed_exercises),
            'completion_rate': completion_rate,
            'average_score': avg_score,
            'current_module': self.progress_data.current_module,
            'current_exercise': self.progress_data.current_exercise,
            'modules_completed': self.progress_data.modules_completed,
            'achievements': self.progress_data.achievements,
            'recent_activity': self.progress_data.progress_entries[-5:] if self.progress_data.progress_entries else []
        }

    def get_next_recommendation(self) -> Dict[str, str]:
        """Get recommendation for what to work on next."""
        # Simple recommendation logic
        current_module = self.progress_data.current_module
        current_exercise = self.progress_data.current_exercise

        # Check if current exercise is completed
        current_completed = any(
            entry.module == current_module and
            entry.exercise_id == current_exercise and
            entry.status == 'completed'
            for entry in self.progress_data.progress_entries
        )

        if current_completed:
            return {
                'type': 'continue',
                'message': f"Great job completing {current_module}/{current_exercise}! Ready for the next challenge?",
                'next_module': self.progress_data.current_module,
                'next_exercise': 'exercise-2'  # Simplified
            }
        else:
            return {
                'type': 'retry',
                'message': f"Keep working on {current_module}/{current_exercise}. You're making progress!",
                'current_module': current_module,
                'current_exercise': current_exercise
            }

    def show_achievements(self):
        """Display earned achievements."""
        if not self.progress_data.achievements:
            print("🏆 No achievements yet - keep learning to unlock them!")
            return

        print("🏆 YOUR ACHIEVEMENTS")
        print("=" * 30)

        achievements_info = self.feedback_rules["achievements"]
        for achievement_id in self.progress_data.achievements:
            if achievement_id in achievements_info:
                achievement = achievements_info[achievement_id]
                print(f"{achievement['title']}")
                print(f"   {achievement['description']}")
                print()

    def reset_progress(self):
        """Reset all progress (for testing or starting over)."""
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
        self.progress_data = self._create_new_progress()
        print("✅ Progress reset successfully!")


def main():
    """Main function for progress tracking."""
    parser = argparse.ArgumentParser(description='Track Airflow Kata progress')
    parser.add_argument(
        '--save-progress',
        action='store_true',
        help='Save current progress'
    )
    parser.add_argument(
        '--show-feedback',
        action='store_true',
        help='Show personalized feedback'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show progress summary'
    )
    parser.add_argument(
        '--achievements',
        action='store_true',
        help='Show earned achievements'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset all progress'
    )
    parser.add_argument(
        '--validate-and-track',
        nargs=2,
        metavar=('MODULE', 'EXERCISE'),
        help='Validate exercise and track progress'
    )

    args = parser.parse_args()

    tracker = ProgressTracker()

    if args.reset:
        confirm = input("Are you sure you want to reset all progress? (y/N): ")
        if confirm.lower() == 'y':
            tracker.reset_progress()
        return

    if args.validate_and_track:
        module, exercise = args.validate_and_track

        # Validate the exercise
        result = tracker.validator.validate_exercise(module, exercise)

        # Record the attempt (with estimated time)
        estimated_time = 30  # Default 30 minutes
        tracker.record_exercise_attempt(
            module, exercise, result, estimated_time)

        # Show results
        print(f"\n📝 Exercise Validation: {module}/{exercise}")
        print("=" * 50)
        print(f"Status: {'✅ PASSED' if result.passed else '❌ FAILED'}")
        print(f"Score: {result.score:.1%}")

        # Show personalized feedback
        feedback = tracker.get_personalized_feedback(result)
        if feedback:
            print("\n💬 Feedback:")
            for fb in feedback:
                print(f"   {fb}")

        return

    if args.summary:
        summary = tracker.get_progress_summary()

        print("\n📊 PROGRESS SUMMARY")
        print("=" * 40)
        print(f"Student ID: {summary['student_id']}")
        print(f"Started: {summary['start_date'][:10]}")
        print(f"Days Active: {summary['days_active']}")
        print(f"Total Time: {summary['total_time_spent']} minutes")
        print(f"Avg Time/Day: {summary['avg_time_per_day']:.1f} minutes")
        print()
        print(
            f"Exercises Completed: {summary['completed_exercises']}/{summary['total_attempts']}")
        print(f"Completion Rate: {summary['completion_rate']:.1%}")
        print(f"Average Score: {summary['average_score']:.1%}")
        print()
        print(
            f"Current Position: {summary['current_module']}/{summary['current_exercise']}")

        if summary['achievements']:
            print(f"Achievements: {len(summary['achievements'])}")

        return

    if args.achievements:
        tracker.show_achievements()
        return

    if args.show_feedback:
        recommendation = tracker.get_next_recommendation()
        print(f"\n💡 RECOMMENDATION")
        print("=" * 30)
        print(recommendation['message'])
        return

    # Default: show basic progress
    summary = tracker.get_progress_summary()
    print(f"\n🎯 Current Progress: {summary['completion_rate']:.1%}")
    print(
        f"📍 Working on: {summary['current_module']}/{summary['current_exercise']}")
    print(f"⏱️  Time spent: {summary['total_time_spent']} minutes")

    if summary['achievements']:
        print(f"🏆 Achievements: {len(summary['achievements'])}")

    print(f"\nUse --help to see all available options.")


if __name__ == "__main__":
    main()
