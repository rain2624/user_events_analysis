import os
import json
import random
import uuid
from datetime import datetime, date, timedelta


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = "D:/Rania/user_interactions_data/bronze/user_interactions"

# Historical data:
# 10 days ending 2 days before today
HISTORICAL_END_DATE = date.today() - timedelta(days=1)
HISTORICAL_DAYS = 10

MIN_SESSIONS_PER_DAY = 10_000
MAX_SESSIONS_PER_DAY = 30_000

# RANDOM_SEED = 42
# random.seed(RANDOM_SEED)

# ============================================================
# LATE-ARRIVING DATA
# ============================================================

# Percentage of a normal day's sessions generated as
# late-arriving records for each late-arrival day.
LATE_ARRIVAL_PERCENTAGE = 0.10

# Today's ingestion can contain events that actually occurred
# yesterday or the day before yesterday.
LATE_ARRIVAL_DAYS = [1, 2]


# ============================================================
# SOURCE SYSTEM VALUES
# ============================================================

DEVICES = [
    "mobile",
    "desktop",
    "tablet"
]

PLATFORMS = [
    "android",
    "ios",
    "web"
]

TRAFFIC_SOURCES = [
    "google",
    "facebook",
    "instagram",
    "email",
    "direct",
    "organic"
]

PRODUCTS = [
    f"P{str(i).zfill(4)}"
    for i in range(1, 501)
]

USERS = [
    f"U{str(i).zfill(6)}"
    for i in range(1, 10_001)
]


# ============================================================
# ID GENERATORS
# ============================================================

def generate_event_id():

    return f"EVT{uuid.uuid4().hex[:12].upper()}"


def generate_session_id():

    return f"S{uuid.uuid4().hex[:10].upper()}"


# ============================================================
# CREATE EVENT
# ============================================================

def create_event(
    user_id,
    session_id,
    session_start,
    event_type,
    product_id,
    device_type,
    platform,
    traffic_source,
    event_date
):

    now = datetime.now()

    # --------------------------------------------------------
    # Generate event timestamp
    #
    # Event can occur up to 30 minutes after session start,
    # BUT it can never:
    #
    # 1. Cross into the next day
    # 2. Be in the future
    # --------------------------------------------------------

    day_end = datetime.combine(
        event_date,
        datetime.max.time()
    )

    proposed_event_timestamp = (
        session_start
        + timedelta(
            seconds=random.randint(0, 1_800)
        )
    )

    event_timestamp = min(
        proposed_event_timestamp,
        day_end
    )

    # --------------------------------------------------------
    # For today's data:
    # event_timestamp must never be in the future.
    # --------------------------------------------------------

    if event_date == date.today():

        event_timestamp = min(
            event_timestamp,
            now
        )

    # --------------------------------------------------------
    # Safety check:
    # Event must belong to the requested event_date.
    # --------------------------------------------------------

    if event_timestamp.date() != event_date:

        raise ValueError(
            f"Invalid event timestamp: "
            f"{event_timestamp} for event_date={event_date}"
        )

    # --------------------------------------------------------
    # Ingestion timestamp
    #
    # Normal ingestion delay: 5-300 seconds.
    #
    # For today's events, ingestion_timestamp cannot
    # be in the future.
    # --------------------------------------------------------

    proposed_ingestion_timestamp = (
        event_timestamp
        + timedelta(
            seconds=random.randint(5, 300)
        )
    )

    if event_date == date.today():

        ingestion_timestamp = min(
            proposed_ingestion_timestamp,
            now
        )

    else:

        ingestion_timestamp = (
            proposed_ingestion_timestamp
        )

    # --------------------------------------------------------
    # Final event
    # --------------------------------------------------------

    return {
        "event_id": generate_event_id(),

        "user_id": user_id,

        "session_id": session_id,

        "event_timestamp": (
            event_timestamp.isoformat()
        ),

        "ingestion_timestamp": (
            ingestion_timestamp.isoformat()
        ),

        "event_type": event_type,

        "product_id": product_id,

        "device_type": device_type,

        "platform": platform,

        "traffic_source": traffic_source,

        "country": "India"
    }


# ============================================================
# GENERATE ONE SESSION
# ============================================================

def generate_session(event_date):

    user_id = random.choice(USERS)

    session_id = generate_session_id()

    start_time = datetime.combine(
        event_date,
        datetime.min.time()
    )

    # --------------------------------------------------------
    # TODAY
    #
    # Session start can only be between midnight and now.
    #
    # This prevents future events.
    # --------------------------------------------------------

    if event_date == date.today():

        now = datetime.now()

        seconds_available = int(
            (now - start_time).total_seconds()
        )

        max_seconds = max(
            0,
            seconds_available
        )

    # --------------------------------------------------------
    # HISTORICAL DATE
    #
    # Session can occur anywhere during that day.
    # --------------------------------------------------------

    else:

        max_seconds = 86_399

    session_start = (
        start_time
        + timedelta(
            seconds=random.randint(
                0,
                max_seconds
            )
        )
    )

    device_type = random.choice(
        DEVICES
    )

    platform = random.choice(
        PLATFORMS
    )

    traffic_source = random.choice(
        TRAFFIC_SOURCES
    )

    product_id = random.choice(
        PRODUCTS
    )

    events = []

    # ========================================================
    # PAGE VIEW
    # ========================================================

    events.append(
        create_event(
            user_id,
            session_id,
            session_start,
            "page_view",
            None,
            device_type,
            platform,
            traffic_source,
            event_date
        )
    )

    # ========================================================
    # PRODUCT VIEW
    # ========================================================

    if random.random() < 0.70:

        number_of_views = random.randint(
            1,
            4
        )

        for _ in range(number_of_views):

            events.append(
                create_event(
                    user_id,
                    session_id,
                    session_start,
                    "product_view",
                    product_id,
                    device_type,
                    platform,
                    traffic_source,
                    event_date
                )
            )

        # ====================================================
        # ADD TO CART
        # ====================================================

        if random.random() < 0.35:

            events.append(
                create_event(
                    user_id,
                    session_id,
                    session_start,
                    "add_to_cart",
                    product_id,
                    device_type,
                    platform,
                    traffic_source,
                    event_date
                )
            )

            # =================================================
            # CHECKOUT
            # =================================================

            if random.random() < 0.65:

                events.append(
                    create_event(
                        user_id,
                        session_id,
                        session_start,
                        "checkout",
                        product_id,
                        device_type,
                        platform,
                        traffic_source,
                        event_date
                    )
                )

                # =============================================
                # PURCHASE
                # =============================================

                if random.random() < 0.75:

                    events.append(
                        create_event(
                            user_id,
                            session_id,
                            session_start,
                            "purchase",
                            product_id,
                            device_type,
                            platform,
                            traffic_source,
                            event_date
                        )
                    )

    return events


# ============================================================
# SOURCE SYSTEM ISSUES
# ============================================================

def apply_source_issues(events):

    output = []

    for event in events:

        probability = random.random()

        # ----------------------------------------------------
        # Missing user ID
        # 0.5%
        # ----------------------------------------------------

        if probability < 0.005:

            event["user_id"] = None

        # ----------------------------------------------------
        # Missing session ID
        # 0.5%
        # ----------------------------------------------------

        elif probability < 0.010:

            event["session_id"] = None

        # ----------------------------------------------------
        # Unknown traffic source
        # 0.5%
        # ----------------------------------------------------

        elif probability < 0.015:

            event["traffic_source"] = "unknown"

        # ----------------------------------------------------
        # Invalid purchase
        # Purchase should have product_id
        # 0.3%
        # ----------------------------------------------------

        elif probability < 0.018:

            if event["event_type"] == "purchase":

                event["product_id"] = None

        # ----------------------------------------------------
        # Missing event timestamp
        # 0.1%
        # ----------------------------------------------------

        elif probability < 0.019:

            event["event_timestamp"] = None

        output.append(event)

        # ----------------------------------------------------
        # Duplicate event
        #
        # Same event_id represents API/client retry.
        #
        # This remains intentional.
        # ----------------------------------------------------

        if random.random() < 0.01:

            duplicate = event.copy()

            output.append(duplicate)

    return output


# ============================================================
# GENERATE ONE DAY
# ============================================================

def generate_day(event_date):

    # --------------------------------------------------------
    # Safety check
    # --------------------------------------------------------

    if event_date > date.today():

        raise ValueError(
            f"Cannot generate future data: {event_date}"
        )

    sessions_per_day = random.randint(
        MIN_SESSIONS_PER_DAY,
        MAX_SESSIONS_PER_DAY
    )

    print(
        f"Generating {event_date} "
        f"with {sessions_per_day:,} sessions"
    )

    events = []

    for _ in range(sessions_per_day):

        session_events = generate_session(
            event_date
        )

        events.extend(
            session_events
        )

    print(
        f"Base events generated: "
        f"{len(events):,}"
    )

    # --------------------------------------------------------
    # Apply intentional source-system issues
    # --------------------------------------------------------

    events = apply_source_issues(
        events
    )

    # --------------------------------------------------------
    # Shuffle events.
    #
    # This simulates out-of-order records in the source file.
    # It does NOT create late-arriving event dates.
    # --------------------------------------------------------

    random.shuffle(
        events
    )

    return events


# ============================================================
# GENERATE LATE-ARRIVING DATA
# ============================================================

def generate_late_arriving_data(ingestion_date):

    late_events = []

    # Generate a smaller portion of data for each late-arrival day.
    base_sessions = random.randint(
        MIN_SESSIONS_PER_DAY,
        MAX_SESSIONS_PER_DAY
    )

    for days_late in LATE_ARRIVAL_DAYS:

        original_event_date = (
            ingestion_date - timedelta(days=days_late)
        )

        late_sessions = int(
            base_sessions * LATE_ARRIVAL_PERCENTAGE
        )

        print(
            f"Generating {late_sessions:,} late-arriving sessions "
            f"for event_date={original_event_date} "
            f"being ingested on {ingestion_date}"
        )

        events = []

        for _ in range(late_sessions):

            session_events = generate_session(
                original_event_date
            )

            events.extend(session_events)

        # Apply the same source-system issues as normal data.
        events = apply_source_issues(events)

        # These events happened on original_event_date,
        # but the source system sends them today.
        ingestion_timestamp = datetime.now()

        for event in events:

            event["ingestion_timestamp"] = (
                ingestion_timestamp.isoformat()
            )

        late_events.extend(events)

    random.shuffle(late_events)

    return late_events


# ============================================================
# SAVE DAILY INGESTION FILE
# ============================================================

def save_daily_ingestion_file(events, ingestion_date):

    # IMPORTANT:
    # The file is stored according to ingestion date, NOT
    # event_date. This allows one ingestion file to contain
    # today's events plus late-arriving events from previous days.

    output_path = os.path.join(
        OUTPUT_DIR,
        f"ingestion_date={ingestion_date}"
    )

    os.makedirs(
        output_path,
        exist_ok=True
    )

    file_path = os.path.join(
        output_path,
        f"user_interactions_{ingestion_date}.json"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        for event in events:

            file.write(
                json.dumps(
                    event,
                    separators=(",", ":")
                )
            )

            file.write("\n")

    print(
        f"Final records written: "
        f"{len(events):,}"
    )

    print(
        f"File: {file_path}"
    )


# ============================================================
# SAVE ONE DAY
# ============================================================

def save_day(events, event_date):

    output_path = os.path.join(
        OUTPUT_DIR,
        f"event_date={event_date}"
    )

    os.makedirs(
        output_path,
        exist_ok=True
    )

    file_path = os.path.join(
        output_path,
        f"user_interactions_{event_date}.json"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        for event in events:

            file.write(
                json.dumps(
                    event,
                    separators=(",", ":")
                )
            )

            file.write("\n")

    print(
        f"Final records written: "
        f"{len(events):,}"
    )

    print(
        f"File: {file_path}"
    )


# ============================================================
# HISTORICAL DATA
# ============================================================

def generate_historical_data():

    historical_start_date = (
        HISTORICAL_END_DATE
        - timedelta(
            days=HISTORICAL_DAYS - 1
        )
    )

    print("=" * 60)
    print("GENERATING HISTORICAL DATA")
    print(
        f"From: {historical_start_date}"
    )
    print(
        f"To:   {HISTORICAL_END_DATE}"
    )
    print("=" * 60)

    for day_number in range(
        HISTORICAL_DAYS
    ):

        event_date = (
            historical_start_date
            + timedelta(
                days=day_number
            )
        )

        events = generate_day(
            event_date
        )

        save_day(
            events,
            event_date
        )

        print("-" * 60)


# ============================================================
# DAILY DATA + LATE-ARRIVING DATA
# ============================================================

def generate_daily_data(target_date):

    # --------------------------------------------------------
    # Daily generator is intended for TODAY only.
    # --------------------------------------------------------

    today = date.today()

    if target_date != today:

        raise ValueError(
            f"Daily generator only generates today's data. "
            f"Expected {today}, received {target_date}"
        )

    print("=" * 60)
    print(
        f"GENERATING TODAY'S DATA: {target_date}"
    )
    print("=" * 60)

    # ========================================================
    # 1. NORMAL TODAY'S DATA
    # ========================================================

    today_events = generate_day(
        target_date
    )

    print(
        f"Today's events: {len(today_events):,}"
    )

    # ========================================================
    # 2. LATE-ARRIVING DATA
    # ========================================================

    late_events = generate_late_arriving_data(
        target_date
    )

    print(
        f"Late-arriving events: {len(late_events):,}"
    )

    # ========================================================
    # 3. COMBINE TODAY + LATE DATA
    # ========================================================

    all_events = (
        today_events
        + late_events
    )

    random.shuffle(
        all_events
    )

    print(
        f"Total events in today's ingestion file: "
        f"{len(all_events):,}"
    )

    # ========================================================
    # 4. SAVE BY INGESTION DATE
    # ========================================================

    save_daily_ingestion_file(
        all_events,
        target_date
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    # ========================================================
    # RUN 1 — HISTORICAL LOAD
    # ========================================================
    #
    # Generates 10 historical days ending 2 days before today.
    #
    # Example if today = 2026-09-08:
    #
    # 2026-08-28
    # 2026-08-29
    # 2026-08-30
    # ...
    # 2026-09-06
    #
    # ========================================================

    generate_historical_data()


    # ========================================================
    # RUN 2 — TODAY
    # ========================================================
    #
    # Generates:
    #   1. Today's events
    #   2. Late-arriving events whose event_date is
    #      yesterday
    #   3. Late-arriving events whose event_date is
    #      the day before yesterday
    #
    # The JSON schema remains unchanged.
    #
    # Example if today = 2026-09-25:
    #
    # event_date        ingestion_timestamp
    # 2026-09-25        2026-09-25  <-- normal
    # 2026-09-24        2026-09-25  <-- late
    # 2026-09-23        2026-09-25  <-- late
    #
    # ========================================================

    # generate_daily_data(
    #     date.today() 
    # )
