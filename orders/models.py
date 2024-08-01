from django.db import models
from django.contrib.auth import get_user_model
from home.models import Product


class Order(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE, related_name='orders')
    paid = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


    class Meta:
        ordering = ('paid', '-updated_at')


    def __str__(self) -> str:
        return f'{self.user} - {str(self.id)}'
    
    def get_total_price(self):
        return sum([item.get_cost() for item in self.items.all()])


class OrderItem(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='items')
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    price = models.IntegerField()
    quantity = models.IntegerField(default=1)


    def __str__(self) -> str:
        return str(self.id)
    
    def get_cost(self):
        return self.price * self.quantity