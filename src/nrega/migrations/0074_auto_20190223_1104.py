# Generated by Django 2.1.3 on 2019-02-23 05:34

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0073_auto_20190222_2237'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='workpayment',
            unique_together=set(),
        ),
        migrations.RemoveField(
            model_name='workpayment',
            name='muster',
        ),
        migrations.RemoveField(
            model_name='workpayment',
            name='worker',
        ),
        migrations.DeleteModel(
            name='WorkPayment',
        ),
    ]