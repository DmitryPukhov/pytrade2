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
    take_profit_price: Mapped[float]
    stop_loss_order_id: Mapped[str] = Column(String, default=None)
    trailing_delta: Mapped[float] = Column(Float, default=None)
    quantity: Mapped[int]
    close_time: Mapped[DateTime] = Column(DateTime, default=None)
    close_price: Mapped[float] = Column(Float, default=None)
    close_order_id: Mapped[str] = Column(String, default=None)

    order_side_names = {1: "BUY", -1: "SELL"}
    order_side_codes = dict(map(reversed, order_side_names.items()))

    def __str__(self):
        details = f"{self.ticker} {self.side}, at {self.open_time} price: {self.open_price}, " \
                  f"sl: {self.stop_loss_price}, tp: {self.take_profit_price}"
        if self.close_time:
            close_details = f", closed: {self.close_price} at {self.close_time}"
            if self.close_order_id == self.stop_loss_order_id:
                close_details += " by stop loss"
            else:
                close_details += " by robot"
            details += close_details
        return details

    def direction(self):
        return Trade.order_side_codes.get(self.side)
