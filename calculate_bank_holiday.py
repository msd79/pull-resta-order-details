from datetime import date, timedelta

def calculate_easter(year):
    """Calculate Easter Sunday for a given year using the Computus algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)

def is_bank_holiday(check_date):
    """
    Check if a given date is a bank holiday in England.
    
    Args:
        check_date (date): The date to check.
    
    Returns:
        bool: True if the date is a bank holiday, False otherwise.
    """
    year = check_date.year
    
    # Fixed date holidays
    fixed_holidays = {
        date(year, 1, 1),  # New Year's Day
        date(year, 12, 25),  # Christmas Day
        date(year, 12, 26),  # Boxing Day
    }
    
    # Calculate Easter-based holidays
    easter_sunday = calculate_easter(year)
    good_friday = easter_sunday - timedelta(days=2)
    easter_monday = easter_sunday + timedelta(days=1)
    
    # Calculate other holidays
    early_may_bank_holiday = date(year, 5, 1)
    while early_may_bank_holiday.weekday() != 0:
        early_may_bank_holiday += timedelta(days=1)
    
    spring_bank_holiday = date(year, 5, 31)
    while spring_bank_holiday.weekday() != 0:
        spring_bank_holiday -= timedelta(days=1)
    
    summer_bank_holiday = date(year, 8, 31)
    while summer_bank_holiday.weekday() != 0:
        summer_bank_holiday -= timedelta(days=1)
    
    # Combine all holidays
    all_holidays = fixed_holidays.union({
        good_friday,
        easter_monday,
        early_may_bank_holiday,
        spring_bank_holiday,
        summer_bank_holiday
    })
    
    # Substitute holidays if they fall on a weekend
    for holiday in list(all_holidays):
        if holiday.weekday() >= 5:  # Saturday or Sunday
            all_holidays.add(holiday + timedelta(days=(7 - holiday.weekday())))
    
    return check_date in all_holidays
