from typing import Optional

from sqlalchemy import DateTime, Column, Float, String, INT, BigInteger, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import extract

class Base(DeclarativeBase):
    pass


class Trade(Base):
    """
    Trade is a pair of orders: open and close.
    """

    __tablename__ = "trade"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String)
    side: Mapped[str] = mapped_column(String)
    open_time: Mapped[DateTime] = Column(DateTime)
    open_price: Mapped[float] = mapped_column(Float)
    open_order_id: Mapped[str] = mapped_column(String)
    stop_loss_price: Mapped[float] = mapped_column(nullable=True)
    take_profit_price: Mapped[float] = mapped_column(nullable=True)
    stop_loss_order_id: Mapped[str] = mapped_column(nullable=True)
    trailing_delta: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column()
    close_time: Mapped[DateTime] = Column(DateTime, nullable=True)
    close_price: Mapped[float] = mapped_column(nullable=True)
    close_order_id: Mapped[str] = mapped_column(nullable=True)

    order_side_names = {1: "BUY", -1: "SELL"}
    order_side_codes = dict(map(reversed, order_side_names.items()))

    def __str__(self):
        details = f"{self.ticker} {self.side}, open time: {self.open_time}, open price: {self.open_price}, " \
                  f"sl: {self.stop_loss_price}, tp: {self.take_profit_price}"
        if self.close_time:
            profit = None
            if self.side == "BUY":
                profit = (self.close_price - self.open_price)*self.quantity
            elif self.side == "SELL":
                profit = (self.open_price - self.close_price)*self.quantity
            close_details = f", close time: {self.close_time}, close price: {self.close_price}, profit: {profit}"
            details += close_details
        return details

    def open_time_epoch_millis(self):
        return extract('epoch', self.open_time) * 1000

    def direction(self):
        return Trade.order_side_codes.get(self.side)
