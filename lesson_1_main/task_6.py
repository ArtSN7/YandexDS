import csv
from datetime import datetime, timedelta


# Function to add 30 minutes to a timestamp
def add_30_min(time):
    dt = datetime.fromisoformat(time)
    dt_plus_30 = dt + timedelta(minutes=30)
    return dt_plus_30.isoformat()


# Function to compute time ranges with concurrent sessions
def get_timings(sessions):
    events = []
    for session_id, (start, end) in sessions.items():
        events.append((datetime.fromisoformat(start), 1))  # Start event
        events.append((datetime.fromisoformat(end), -1))   # End event

    # Sort events by time, then by event type (end before start for same timestamp)
    events.sort(key=lambda x: (x[0], x[1]))

    for i in events:
        print(i)

    result = []
    current_concurrent = 0
    prev_time = None

    for time, event_type in events:
        # If there’s at least one active session, record the range
        if prev_time is not None and current_concurrent > 0:
            result.append({
                'start': prev_time.isoformat(),
                'end': time.isoformat(),
                'concurrent_sessions': current_concurrent
            })

        current_concurrent += event_type
        prev_time = time

    return result


def main():
    start_end_all_sessions = {}  # session_id: [start, end + 30 min]

    # Read the TSV file
    with open('log.tsv', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Skip header

        for row in reader:
            session_id = row[5]
            timestamp = row[1]

            if session_id not in start_end_all_sessions:
                # First event for this session
                start_end_all_sessions[session_id] = [timestamp, add_30_min(timestamp)]
            else:
                # Update end time to 30 minutes after the latest event
                start_end_all_sessions[session_id][1] = add_30_min(timestamp)

    # Get time ranges with concurrent sessions
    time_ranges = get_timings(start_end_all_sessions)

    # Find the maximum number of concurrent sessions
    if not time_ranges:
        return  # No sessions to process

    max_sessions = max(item['concurrent_sessions'] for item in time_ranges)

    # Filter ranges with maximum concurrent sessions
    result = [item for item in time_ranges if item['concurrent_sessions'] == max_sessions]

    # Sort by start time
    result.sort(key=lambda x: datetime.fromisoformat(x['start']))

    # Print results
    for item in result:
        print(f"{item['start']} {item['end']}")


if __name__ == '__main__':
    main()
