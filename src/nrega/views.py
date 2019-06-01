from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from django.views.generic import TemplateView
from django.views.generic import View
from django.db.models import F,Q,Sum,Count
from .models import CrawlQueue,Location,Info
from nrega.crawler.events.jan2019PressRelease import getCrawlStatus
import altair as alt
import pandas as pd
from vega_datasets import data
import collections
def makehash():
  return collections.defaultdict(makehash)
# Create your views here.

class crawlStatusView1(View):
  def get(self,request,*args,**kwargs):
    myhtml=getCrawlStatus()
    return render(request,"index2.html",{})
    return HttpResponse(myhtml)

def getInfo(locationCode,statName,finyear=None,inverse=False):
  min=100
  max=0
  if ( ( locationCode is not None) and (locationCode != '')):
    if finyear is not None:
      infos=Info.objects.filter(slug=statName,location__parentLocation__code=locationCode,finyear=finyear)
    else:
      infos=Info.objects.filter(slug=statName,location__parentLocation__code=locationCode)
  else:
    if finyear is not None:
      infos=Info.objects.filter(slug=statName,location__locationType='state',finyear=finyear)
    else:
      infos=Info.objects.filter(slug=statName,location__locationType='state')
  d=[]
  for info in infos:
    p={}
    if inverse==True:
      p[statName]=100-int(info.value)
    else:
      p[statName]=int(info.value)
      if min > int(info.value):
        min=int(info.value)
      if max < int(info.value):
        max=int(info.value)
    p['finyear']=info.finyear
    p['name']=info.location.name
    d.append(p)
  if len(d) > 0:
    source=pd.DataFrame(d)
  else:
    source=None
  return source,min,max


class stateComparison(TemplateView):
  template_name = 'stateComparison.html'
  def get(self, request, *args, **kwargs):
    locations=Location.objects.filter(  Q(locationType = 'district') | Q (locationType = 'state1')).values("code","displayName")
    context = locals()
    context['locations'] = locations
    q = request.GET.get('location',None)
    myInfos=Info.objects.filter(location__locationType = "state").values("slug").annotate(icount=Count("pk"))
    statArray=[]
    for myInfo in myInfos:
      statName=myInfo["slug"]
    #  if "url" not in statName.lower():
      if "payments-gererated-within-15-days" in statName:
        statArray.append(statName)
    chartArray=[]
    for statSlug in statArray:
      print(statSlug)
      d,min,max=getInfo(q,statSlug,finyear='19')
      if min-3 < 0:
        min=0
      else:
        min=min-3
      if max+3 > 100:
        max=100
      else:
        max=max+3
      if d is not None:
        myChart = alt.Chart(d).mark_bar().encode(
         # y=statSlug,
         alt.Y(statSlug,
           scale=alt.Scale(
           domain=(min, max),
           clamp=True
          )
          ),
          x='name',
          color='name'
        ).properties(width=500)
        p={}
        p['name']=statSlug
        p['chart']=myChart
        chartArray.append(p)
    context['chartArray']=chartArray
      
    return render(request, self.template_name, context)
    slug='payments-gererated-within-15-days'
    delayPaymentChart=getInfo(q,slug,finyear='19',inverse=True)
    context['delayPaymentChart'] = alt.Chart(delayPaymentChart).mark_bar().encode(
        y=slug,
        x='name'
    )

    slug="women-persondays-out-of-total"
    womenWorkForce=getInfo(q,slug)
    if womenWorkForce is not None:
      context['womenWorkForceChart'] = alt.Chart(womenWorkForce).mark_bar().encode(
          y=slug,
          x='name'
      )

    slug="isc-worker-against-active-workers"
    d=getInfo(q,slug)
    if d is not None:
      context['SCWorkerChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      )

    slug="iist-worker-against-active-workers"
    d=getInfo(q,slug)
    if d is not None:
      context['STWorkerChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      )


    slug="average-days-of-employment-provided-per-household"
    d=getInfo(q,slug)
    if d is not None:
      context['avgEmploymentChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      ).properties(width=1000)
    return render(request, self.template_name, context)


class autoComplete(TemplateView):
  template_name = 'autocomplete.html'
  def get(self, request, *args, **kwargs):
    locations=Location.objects.filter(  Q(locationType = 'district') | Q (locationType = 'state1')).values("code","displayName")
    context = locals()
    context['locations'] = locations
    q = request.GET.get('location',None)

#   rejectedSource=getInfo(q,"bankRejectedPercentage")
#   context['rejectedPercentageChart'] = alt.Chart(rejectedSource).mark_line().encode(
#       x='finyear',
#       y='bankRejectedPercentage',
#       color='name'
#   ).properties(width=500)
#% payments gererated within 15 days
    slug='payments-gererated-within-15-days'
    delayPaymentChart=getInfo(q,slug,finyear='19',inverse=True)
    context['delayPaymentChart'] = alt.Chart(delayPaymentChart).mark_bar().encode(
        y=slug,
        x='name'
    )

    slug="women-persondays-out-of-total"
    womenWorkForce=getInfo(q,slug)
    if womenWorkForce is not None:
      context['womenWorkForceChart'] = alt.Chart(womenWorkForce).mark_bar().encode(
          y=slug,
          x='name'
      )

    slug="isc-worker-against-active-workers"
    d=getInfo(q,slug)
    if d is not None:
      context['SCWorkerChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      )

    slug="iist-worker-against-active-workers"
    d=getInfo(q,slug)
    if d is not None:
      context['STWorkerChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      )


    slug="average-days-of-employment-provided-per-household"
    d=getInfo(q,slug)
    if d is not None:
      context['avgEmploymentChart'] = alt.Chart(d).mark_bar().encode(
          y=slug,
          x='name'
      )
#   d=[{"Acceleration": 12.0, "Cylinders": 8, "Displacement": 307.0, "Horsepower": 130.0, "Miles_per_Gallon": 18.0, "Name": "chevrolet chevelle malibu", "Origin": "USA", "Weight_in_lbs": 3504, "Year": "1970-01-01T00:00:00"}, {"Acceleration": 11.5, "Cylinders": 8, "Displacement": 350.0, "Horsepower": 165.0, "Miles_per_Gallon": 15.0, "Name": "buick skylark 320", "Origin": "USA", "Weight_in_lbs": 3693, "Year": "1970-01-01T00:00:00"}, {"Acceleration": 11.0, "Cylinders": 8, "Displacement": 318.0, "Horsepower": 150.0, "Miles_per_Gallon": 18.0, "Name": "plymouth satellite", "Origin": "USA", "Weight_in_lbs": 3436, "Year": "1970-01-01T00:00:00"}]
#   source=pd.DataFrame(d)
#   context['chart2'] = alt.Chart(source).mark_circle().encode(
#       x='Horsepower',
#       y='Miles_per_Gallon',
#       color='Origin'
#   ).interactive()
    return render(request, self.template_name, context)

  
class crawlStatusView(TemplateView):
    template_name = 'index_da.html'

    def get(self, request, *args, **kwargs):
        context = locals()
        source = data.cars()
        
        context['chart2'] = alt.Chart(source).mark_circle().encode(
            x='Horsepower',
            y='Miles_per_Gallon',
            color='Origin'
        ).interactive()
        source = pd.DataFrame({
          'a': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'],
           'b': [28, 55, 43, 91, 81, 53, 19, 87, 52]
        })

        context['chart2']=alt.Chart(source).mark_bar().encode(
         x='a',
         y='b'
        )
        return render(request, self.template_name, context)

def my_cool_chart_view(req):
    # import ipdb; ipdb.set_trace()
    # import ipdb; ipdb.set_trace()
    source = data.cars()
    print(source)
    chart = alt.Chart(source).mark_circle().encode(
        x='Horsepower',
        y='Miles_per_Gallon',
        color='Origin'
    ).interactive()
    chart.save("test.html")
    print("saving the chart")
    return JsonResponse(chart.to_dict(), safe=False)


# Create your views here.
def index(request):
    return render(request, "index.html", {})
