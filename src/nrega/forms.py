from django import forms
from .models import LibtechTag,Muster,GenericReport

class LibtechTagForm(forms.ModelForm):
  class Meta:
    model = LibtechTag
    fields = [ 
            'name',
            'description'
            ]
  def clean(self,*args,**kwargs):
    data=self.cleaned_data
    name=data.get("name",None)
    description=data.get("description",None)
    if name is None and description is None:
      raise forms.ValidationError('Name of description required')
    return super().clean(*args,**kwargs)



class GenericReportForm(forms.ModelForm):
  class Meta:
    model = GenericReport
    fields = [
        'name',
        'description',
        'libtechTag',
        ]
  def clean(self,*args,**kwargs):
    data=self.cleaned_data
    name=data.get("name",None)
    description=data.get("description",None)
    if name is None and description is None:
      raise forms.ValidationError('Name or description required')
    return super().clean(*args,**kwargs)



