from django.shortcuts import render, get_object_or_404, redirect, HttpResponse
from django.views import View
from .cart import Cart
from home.models import Product
from .forms import CartAddForm
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
        order = get_object_or_404(Order, id=order_id)
        return render(request, 'orders/order.html', {'order': order})
    

class OrderCreateView(LoginRequiredMixin, View):
     
    def get(self, request):
        cart = Cart(request)
        order = Order.objects.create(user = request.user)
        for item in cart:
            OrderItem.objects.create(order=order, product=item['product'], price=item['price'], quantity=item['quantity'])
        cart.clear()
        return redirect('orders:order_detail', order.id)
    





# zarinpal Configuration

MERCHANT = "01010101-0101-0101-0101-010101010101"

ZP_API_REQUEST = "https://sandbox.zarinpal.com/pg/v4/payment/request.json"
ZP_API_VERIFY = "https://sandbox.zarinpal.com/pg/v4/payment/verify.json"
ZP_API_STARTPAY = "https://sandbox.zarinpal.com/pg/StartPay/"
description = "خرید لوازم جانبی"  # Required

CallbackURL = 'http://127.0.0.1:8080/orders/verify/'

 



class OrderPayView(LoginRequiredMixin, View):
     
    def get(self, request, order_id):
        order = Order.objects.get(id = order_id)
        data = {
        "MerchantID": MERCHANT,
        "Amount": order.get_total_price(),
        "Description": description,
        "Phone": request.user.phone_number,
        "CallbackURL": CallbackURL,
        }
        data = json.dumps(data)
        headers = {'content-type': 'application/json', 'content-length': str(len(data)) }
        try:
            response = requests.post(ZP_API_REQUEST, data=data, headers=headers, timeout=10)

            if response.status_code == 200:
                response = response.json()
                if response['Status'] == 100:
                    return {'status': True, 'url': ZP_API_STARTPAY + str(response['Authority']), 'authority': response['Authority']}
                else:
                    return {'status': False, 'code': str(response['Status'])}
            return response
        
        except requests.exceptions.Timeout:
            return {'status': False, 'code': 'timeout'}
        except requests.exceptions.ConnectionError:
            return {'status': False, 'code': 'connection error'}
        

class OrderVerifyView(LoginRequiredMixin, View):
	def get(self, request):
		order_id = request.session['order_pay']['order_id']
		order = Order.objects.get(id=int(order_id))
		t_status = request.GET.get('Status')
		t_authority = request.GET['Authority']
		if request.GET.get('Status') == 'OK':
			req_header = {"accept": "application/json",
						  "content-type": "application/json'"}
			req_data = {
				"merchant_id": MERCHANT,
				"amount": order.get_total_price(),
				"authority": t_authority
			}
			req = requests.post(url=ZP_API_VERIFY, data=json.dumps(req_data), headers=req_header)
			if len(req.json()['errors']) == 0:
				t_status = req.json()['data']['code']
				if t_status == 100:
					order.paid = True
					order.save()
					return HttpResponse('Transaction success.\nRefID: ' + str(
						req.json()['data']['ref_id']
					))
				elif t_status == 101:
					return HttpResponse('Transaction submitted : ' + str(
						req.json()['data']['message']
					))
				else:
					return HttpResponse('Transaction failed.\nStatus: ' + str(
						req.json()['data']['message']
					))
			else:
				e_code = req.json()['errors']['code']
				e_message = req.json()['errors']['message']
				return HttpResponse(f"Error code: {e_code}, Error Message: {e_message}")
		else:
			return HttpResponse('Transaction failed or canceled by user')