"""
Helper functions for payment reminder functionality
Calculates working days excluding Sundays and 2nd/4th Saturdays
"""

from datetime import datetime, timedelta
import calendar

def is_working_day(date):
    """
    Check if a date is a working day (not Sunday, not 2nd/4th Saturday)
    
    Args:
        date: datetime object
    
    Returns:
        bool: True if working day, False otherwise
    """
    # Check if Sunday (weekday() returns 6 for Sunday)
    if date.weekday() == 6:
        return False
    
    # Check if Saturday (weekday() returns 5 for Saturday)
    if date.weekday() == 5:
        # Get which Saturday of the month this is
        day = date.day
        # Calculate which occurrence of Saturday this is in the month
        # First Saturday is days 1-7, second is 8-14, third is 15-21, fourth is 22-28, fifth is 29-31
        saturday_occurrence = (day - 1) // 7 + 1
        
        # 2nd and 4th Saturdays are holidays
        if saturday_occurrence in [2, 4]:
            return False
    
    return True

def get_working_days_before(target_date, num_working_days):
    """
    Get the date that is N working days before the target date
    
    Args:
        target_date: datetime object or string in 'YYYY-MM-DD' format
        num_working_days: number of working days to go back
    
    Returns:
        datetime object representing the date N working days before
    """
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d')
    
    current_date = target_date
    working_days_counted = 0
    
    # Go back day by day, counting only working days
    while working_days_counted < num_working_days:
        current_date = current_date - timedelta(days=1)
        if is_working_day(current_date):
            working_days_counted += 1
    
    return current_date

def format_date_indian(date):
    """
    Format date in Indian format (DD-MM-YYYY)
    
    Args:
        date: datetime object or string in 'YYYY-MM-DD' format
    
    Returns:
        str: Date in DD-MM-YYYY format
    """
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')
    
    return date.strftime('%d-%m-%Y')

def get_balance_date_for_reminder(reminder_date):
    """
    Get the balance date (4 working days before reminder date)
    
    Args:
        reminder_date: string in 'YYYY-MM-DD' format
    
    Returns:
        tuple: (balance_date as datetime, balance_date as 'YYYY-MM-DD' string)
    """
    balance_date = get_working_days_before(reminder_date, 4)
    return balance_date, balance_date.strftime('%Y-%m-%d')
