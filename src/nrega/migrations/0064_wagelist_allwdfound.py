# Generated by Django 2.1.3 on 2019-02-14 00:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0063_wagelisttransaction_workdetail'),
    ]

    operations = [
        migrations.AddField(
            model_name='wagelist',
            name='allWDFound',
            field=models.BooleanField(default=False),
        ),
    ]
