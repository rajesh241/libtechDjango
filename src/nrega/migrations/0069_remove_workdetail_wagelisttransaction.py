# Generated by Django 2.1.3 on 2019-02-15 00:57

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0068_wagelisttransaction_workdetail'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='workdetail',
            name='wagelistTransaction',
        ),
    ]
