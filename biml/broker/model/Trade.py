import datetime as datetime
import sqlalchemy
from sqlalchemy import DateTime, Column
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
    open_time: Mapped[DateTime] = Column(DateTime, default=datetime.datetime.utcnow)
    open_price: Mapped[float]
    stop_loss_price: Mapped[float]
    take_profit_price: Mapped[float]
    quantity: Mapped[int]
    close_time: Mapped[DateTime] = Column(DateTime, default=datetime.datetime.utcnow)
    close_price: Mapped[float]