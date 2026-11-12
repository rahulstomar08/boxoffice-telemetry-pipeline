from datetime import datetime
from typing import List, Literal
from pydantic import BaseModel, Field, model_validator


# ==========================================
# PART 1: Static Seed Data Models (theatres_seed.json)
# ==========================================

class SeatCategorySeed(BaseModel):
    category_name: str
    base_price_inr: float = Field(gt=0, description="Ticket price in INR, must be > 0")
    total_seats: int = Field(gt=0, description="Total physical seats in this category")


class ScreenSeed(BaseModel):
    screen_id: str
    screen_name: str
    format: str
    seat_categories: List[SeatCategorySeed]


class TheatreSeed(BaseModel):
    theatre_id: str
    theatre_name: str
    chain_name: str
    city_code: Literal["DEL", "BLR"]
    city_name: str
    sub_zone: str
    is_single_screen: bool
    offline_multiplier_weight: float = Field(
        ge=1.0, description="Walk-in extrapolation multiplier (>= 1.0)"
    )
    screens: List[ScreenSeed]


# ==========================================
# PART 2: Live Streaming Snapshot Models (Kafka Payload)
# ==========================================

class SeatCategorySnapshot(BaseModel):
    category_name: str
    base_price_inr: float = Field(gt=0)
    total_seats: int = Field(gt=0)
    booked_seats: int = Field(ge=0)
    blocked_seats: int = Field(ge=0)
    seat_states_bitmask: str

    @model_validator(mode="after")
    def check_seat_capacity_bounds(self) -> "SeatCategorySnapshot":
        if (self.booked_seats + self.blocked_seats) > self.total_seats:
            raise ValueError(
                f"Invalid seat counts in {self.category_name}: "
                f"booked ({self.booked_seats}) + blocked ({self.blocked_seats}) "
                f"cannot exceed total_seats ({self.total_seats})"
            )
        return self


class ShowSeatSnapshotEvent(BaseModel):
    event_id: str
    show_id: str
    theatre_id: str
    city_code: Literal["DEL", "BLR"]
    snapshot_window: Literal["T-24h", "T-6h", "T-2h", "T-30m", "T-10m"]
    snapshot_timestamp: datetime
    show_start_timestamp: datetime
    movie_id: str
    screen_id: str
    seat_categories: List[SeatCategorySnapshot]