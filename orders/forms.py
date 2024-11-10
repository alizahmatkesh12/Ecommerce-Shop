from django import forms
from accounts.models.address import Address

PRODUCT_QUANTITY_CHOICES = [(i, str(i)) for i in range(1, 21)]


class CartAddForm(forms.Form):
    quantity = forms.TypedChoiceField(choices=PRODUCT_QUANTITY_CHOICES, coerce=int)


class AddressSelectForm(forms.Form):
    address = forms.ModelChoiceField(queryset=Address.objects.all(), required=True)

    def __init__(self, user=None, *args, **kwargs):
        user = kwargs.pop('user', None) 
        super().__init__(*args, **kwargs)
        if user:
            self.fields["address"].queryset = Address.objects.filter(user=user)