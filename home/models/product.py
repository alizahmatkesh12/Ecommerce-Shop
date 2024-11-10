from django.db import models
from django.urls import reverse

from .category import Category
from .abstract_model import Extensions


def product_image_path(instance, filename):
    return "product/images/{}/{}".format(instance.title, filename)

class Product(Extensions):
    """
    The Product table contining all product items.
    """
    category = models.ManyToManyField(Category, related_name='products')
    title = models.CharField(max_length=255)
    slug = models.SlugField(max_length=255, unique=True)
    image = models.ImageField(upload_to=product_image_path)
    description = models.TextField()
    price = models.IntegerField()
    available = models.BooleanField(default=True)

    class Meta:
        ordering = ['title']
        verbose_name = ["Product"]
        verbose_name_plural = ["Products"]

    def __str__(self):
        return self.title        

    def get_absolute_url(self):
        return reverse("home:product_detail", args=[self.slug])
