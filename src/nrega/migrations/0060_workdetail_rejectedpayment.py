# Generated by Django 2.1.3 on 2019-02-13 13:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0059_auto_20190213_0558'),
    ]

    operations = [
        migrations.AddField(
            model_name='workdetail',
            name='rejectedPayment',
            field=models.ManyToManyField(blank=True, related_name='wdRejPay', to='nrega.RejectedPayment'),
        ),
    ]
