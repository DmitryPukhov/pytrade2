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
    ticker: Mapped[str] = mapped_column(String)
    side: Mapped[str] = mapped_column(String)
    open_time: Mapped[DateTime] = Column(DateTime)
    open_price: Mapped[float] = mapped_column(Float, nullable=True)
    open_order_id: Mapped[str] = mapped_column(String)
    stop_loss_price: Mapped[float] = mapped_column(nullable=True)
    take_profit_price: Mapped[float] = mapped_column(nullable=True)
    stop_loss_order_id: Mapped[str] = mapped_column(nullable=True)
    trailing_delta: Mapped[float] = mapped_column(nullable=True)
    quantity: Mapped[float] = mapped_column()
    close_time: Mapped[DateTime] = Column(DateTime, nullable=True)
    close_price: Mapped[float] = mapped_column(nullable=True)
    close_order_id: Mapped[str] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String)

    order_side_names = {1: "BUY", -1: "SELL"}
    order_side_codes = dict(map(reversed, order_side_names.items()))

    def __str__(self):
        # Open details
        details = f"{self.ticker} {self.side},  quantity:{self.quantity}, status: {self.status}, " \
                  f"open order id: {self.open_order_id}, open time: {self.open_time}, open price: {self.open_price}, " \
                  f"sl order id: {self.stop_loss_order_id}, sl: {self.stop_loss_price}, " \
                  f"tp: {self.take_profit_price}"

        # Profit
        if self.side == "BUY" and self.close_price and self.open_price and self.quantity:
            profit = (self.close_price - self.open_price) * self.quantity
        elif self.side == "SELL" and self.close_price and self.open_price and self.quantity:
            profit = (self.open_price - self.close_price) * self.quantity
        else:
            profit = None

        # Close details
        close_details = f", close order id: {self.close_order_id}, close time: {self.close_time}," \
                        f"close price: {self.close_price}, profit: {profit}"
        details += close_details
        return details

    def open_time_epoch_millis(self):
        # Timestamp is a float number of seconds, *1000 gets milliseconds
        return int(self.open_time.timestamp() * 1000)

    def direction(self) -> int:
        return Trade.order_side_codes.get(self.side)
