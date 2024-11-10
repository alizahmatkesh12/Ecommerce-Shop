from django.shortcuts import render, get_object_or_404, redirect, HttpResponse
from django.views import View
from django.contrib import messages
from .cart import Cart
from home.models import Product
from .forms import CartAddForm,AddressSelectForm
from django.contrib.auth.mixins import LoginRequiredMixin
from .models import Order, OrderItem
import json
import requests


class CartView(View):
     
	def get(self, request):
		cart = Cart(request)
		return render(request, 'orders/cart.html', {'cart':cart})

    
class CartAddView(View):

    def post(self, request, product_id):
        cart = Cart(request)
        product = get_object_or_404(Product, id=product_id)
        form = CartAddForm(request.POST)
        if form.is_valid():
            cart.add(product, form.cleaned_data['quantity'])
        return redirect('orders:cart')
    

class CartRemoveView(View):

    def get(self, request, product_id):
        cart = Cart(request)
        product = get_object_or_404(Product, id = product_id)
        cart.remove(product)
        return redirect('orders:cart')
            
class OrderDetailView(LoginRequiredMixin, View):

    def get(self, request, order_id):
        # Get the order or return 404 if not found
        order = get_object_or_404(Order, id=order_id)
        
        # Create the form with initial data for the address
        form = AddressSelectForm(user=request.user, initial={'address': order.address})
        
        # Return the order details view
        return render(request, 'orders/order.html', {'order': order, 'form': form})

    def post(self, request, order_id):
        # Get the order or return 404 if not found
        order = get_object_or_404(Order, id=order_id)
        
        # Process the form with POST data
        form = AddressSelectForm(request.POST)
        
        if form.is_valid():
            address = form.cleaned_data['address']
            order.address = address
            order.save()
            return redirect('home:home', order.id)
        
        # If the form is invalid, render the page with errors
        messages.error(request, "Please correct the errors below.")
        return render(request, 'orders/order.html', {'order': order, 'form': form})

class OrderCreateView(LoginRequiredMixin, View):
    
    def get(self, request):
        cart = Cart(request)
        form = AddressSelectForm(user=request.user)  
        return render(request, 'orders/order.html', {'form': form, 'cart': cart})
    
    def post(self, request):
        cart = Cart(request) 
        form = AddressSelectForm(request.POST, data=request.POST)
        
        if form.is_valid():
            address = form.cleaned_data['address']  # This should be an Address object
            
            order = Order.objects.create(user=request.user, address=address)  # Ensure address is an Address object
            for item in cart:
                OrderItem.objects.create(order=order, product=item['product'], price=item['price'], quantity=item['quantity'])
            cart.clear()

            return redirect('orders:order_detail', order.id)
        
        return render(request, 'orders/order.html', {'form': form, 'cart': cart})
    
class OrderPayView(LoginRequiredMixin, View):
     
    def get(self, request, order_id):
        return render(request, "orders/orderPay.html")
        # TODO Payment Integration here.
    def post(self, request, order_id):
        return redirect("home:home")
        # TODO Payment Integration here.

class OrderVerifyView(LoginRequiredMixin, View):
    ...
    # TODO Payment Integration here.