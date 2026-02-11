from abc import abstractmethod, ABC
CUSTOMER_NAMES = [
    "Smith John",
    "Williams Olivia",
    "Brown Michael",
    "Jones Emma",
    "Garcia Daniel",
    "Miller Sophia",
    "Davis Liam",
    "Martinez Ava",
    "Lopez Ethan",
    "Gonzalez Isabella",
    "Wilson Mason",
    "Anderson Mia",
    "Thomas Lucas",
    "Taylor Amelia",
    "Moore Elijah",
    "Jackson Charlotte",
    "Martin Logan",
    "Lee Harper",
    "Perez Jacob",
    "Thompson Evelyn"
]


PRODUCTS = {
    'Phone': 35.0,
    'Laptop': 45.0,
    'Tablet': 30.0,
    'Headphone': 25.0,
    'Smartwatch': 55.0,
    'Camera': 120.0,
    'Speaker': 40.0,
    'Monitor': 80.0,
    'Printer': 50.0,
    'Keyboard': 20.0
}


class UserEvent(ABC):
    event_type='event_type'

    def to_dict(self)->dict:
        pass

class ProductViewEvent(UserEvent):
    def __init__(self, product_name, unit_price, customer_surname, customer_firstname, date_viewed):
        self.event_type = 'product_view'
        self.product_name = product_name
        self.unit_price = unit_price
        self.customer_surname = customer_surname
        self.customer_firstname = customer_firstname
        self.date_viewed = date_viewed


    def to_dict(self):
        return {
            'event_type': self.event_type,
            'product_name': self.product_name,
            'customer_firstname': self.customer_firstname,
            'customer_surname': self.customer_surname,
            'unit_price': self.unit_price,
            'date_viewed': self.date_viewed

        }


    def __repr__(self):
        return f"{self.event_type}- {self.product_name}- {self.customer_surname} {self.customer_firstname}- {self.date_viewed}"

class ProductPurchaseEvent(UserEvent):
    def __init__(self, product_name, customer_surname, customer_firstname, date_purchased, unit_price, quantity):
        self.event_type = 'product_purchase'
        self.product_name = product_name
        self.customer_surname = customer_surname
        self.customer_firstname = customer_firstname
        self.date_purchased = date_purchased
        self.unit_price = unit_price
        self.quantity = quantity

    def to_dict(self):
        return {
            'event_type': self.event_type,
            'product_name': self.product_name,
            'customer_firstname': self.customer_firstname,
            'customer_surname': self.customer_surname,
            'date_purchased': self.date_purchased,
            'unit_price': self.unit_price,
            'quantity': self.quantity
        }
    
    def __repr__(self):
        return f"{self.event_type}- {self.product_name}- {self.customer_name}- {self.date_purchased}- { self.unit_cost}- {self.quantity}"
        

