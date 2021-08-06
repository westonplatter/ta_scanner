from ib_insync import IB, Future, ContFuture, Stock, Contract


def init_client(host: str = "127.0.0.1", port: int = 7496) -> None:
    ib = IB()
    ib.connect(host, port, clientId=0)
    return ib

x = init_client()

print(x)

print("ib_sync import working")