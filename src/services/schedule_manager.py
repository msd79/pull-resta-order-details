# File location: src/services/schedule_manager.py
from datetime import datetime, time
from dataclasses import dataclass
from typing import Optional, Set
from enum import Enum, auto

class Day(Enum):
    """Enum for days of the week"""
    MONDAY = auto()
    TUESDAY = auto()
    WEDNESDAY = auto()
    THURSDAY = auto()
    FRIDAY = auto()
    SATURDAY = auto()
    SUNDAY = auto()

    @classmethod
    def from_datetime(cls, dt: datetime) -> 'Day':
        """Convert datetime.weekday() to Day enum"""
        # datetime weekday() returns 0-6 for Monday-Sunday
        return list(Day)[dt.weekday()]

@dataclass
class ScheduleWindow:
    start_time: time
    end_time: time
    active_days: Set[Day]

class ScheduleManager:
    """Manages application running schedule with day-of-week support"""
    
    def __init__(self, start_hour: int, start_minute: int, end_hour: int, end_minute: int, active_days: list[str]):
        """
        Initialize schedule manager with running hours and days
        
        Args:
            start_hour: Hour to start running (24-hour format)
            start_minute: Minute to start running
            end_hour: Hour to stop running (24-hour format)
            end_minute: Minute to stop running
            active_days: List of days when the application should run (e.g., ['MONDAY', 'TUESDAY'])
        """
        # Convert day strings to Day enum set
        active_day_set = {Day[day.upper()] for day in active_days}
        
        self.schedule = ScheduleWindow(
            start_time=time(start_hour, start_minute),
            end_time=time(end_hour, end_minute),
            active_days=active_day_set
        )
        
    def is_within_schedule(self) -> bool:
        """Check if current time is within scheduled running hours and days"""
        current_dt = datetime.now()
        current_time = current_dt.time()
        current_day = Day.from_datetime(current_dt)
        
        # First check if it's an active day
        if current_day not in self.schedule.active_days:
            return False
            
        # Then check if it's within the time window
        if self.schedule.start_time <= self.schedule.end_time:
            return self.schedule.start_time <= current_time <= self.schedule.end_time
        else:
            # Handle schedule windows that span midnight
            return current_time >= self.schedule.start_time or current_time <= self.schedule.end_time
            
    def time_until_next_window(self) -> float:
        """Calculate seconds until next running window starts"""
        current_dt = datetime.now()
        current_date = current_dt.date()
        current_day = Day.from_datetime(current_dt)
        
        # Find the next active day
        days_ahead = 0
        test_day = current_day
        while days_ahead < 8:  # Check up to 7 days ahead plus today
            if test_day in self.schedule.active_days:
                # If it's today, check if start time hasn't passed
                if days_ahead == 0 and current_dt.time() < self.schedule.start_time:
                    break
                # If it's a future day, use this day
                if days_ahead > 0:
                    break
            days_ahead += 1
            # Get next day's enum value
            test_day = list(Day)[(test_day.value - 1 + 1) % 7]
        
        if days_ahead >= 8:
            raise RuntimeError("No active days found in schedule")
            
        # Calculate the next start datetime
        next_start = datetime.combine(current_date, self.schedule.start_time)
        if days_ahead > 0:
            next_start = next_start.replace(day=next_start.day + days_ahead)
            
        return max(0, (next_start - current_dt).total_seconds())
    
    def should_start_immediately(self) -> bool:
        """Check if the application should start running immediately"""
        return self.is_within_schedule()