from datetime import datetime, timezone
def convert_to_milliseconds(start_time, end_time):
    start_time_utc = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_time_utc = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    
    start_time_ms = int(start_time_utc.timestamp() * 1000)
    end_time_ms = int(end_time_utc.timestamp() * 1000)

    return start_time_ms, end_time_ms