from django.db import models
from django.urls import reverse


class Category(models.Model):
    sub_category = models.ForeignKey('Category', on_delete=models.CASCADE, related_name='scategory', null=True, blank=True)
    is_sub = models.BooleanField(default=False)
    name = models.CharField(max_length=255)
    slug = models.SlugField(max_length=255, unique=True)
    
    
    
    class Meta:
        ordering = ['name']
        verbose_name = 'Category'
        verbose_name_plural = 'Categories'
        
        
    def __str__(self) -> str:
        return self.name
    
    
    def get_absolute_url(self):
        return reverse("home:category_filter", args=[self.slug])
