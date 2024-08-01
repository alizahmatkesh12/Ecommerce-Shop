from django.contrib import admin
from .models import Order, OrderItem


class OrderItemInLine(admin.TabularInline):
    model = OrderItem
    raw_id_fields = ['product']


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
     list_display = ['id', 'user', 'updated_at', 'paid']
     list_filter = ['paid']
     inlines = [OrderItemInLine]