import datetime as datetime
import sqlalchemy
from sqlalchemy import DateTime, Column, Float, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Trade(Base):
    """
    Trade is a pair of orders: open and close.
    """

    __tablename__ = "trade"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column()
    side: Mapped[str] = mapped_column()
    open_time: Mapped[DateTime] = Column(DateTime)
    open_price: Mapped[float]
    open_order_id: Mapped[str]
    stop_loss_price: Mapped[float]
    trailing_delta: Mapped[float] = Column(Float, default=None)
    quantity: Mapped[int]
    close_time: Mapped[DateTime] = Column(DateTime, default=None)
    close_price: Mapped[float] = Column(Float, default=None)
    close_order_id: Mapped[str] = Column(String, default=None)
