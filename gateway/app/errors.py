class ApiError(Exception):
    """Returned to the client as {"error": code} with the given HTTP status."""

    def __init__(self, status: int, code: str):
        super().__init__(code)
        self.status = status
        self.code = code
