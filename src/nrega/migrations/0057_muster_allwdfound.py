# Generated by Django 2.1.3 on 2019-02-12 23:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0056_auto_20190213_0436'),
    ]

    operations = [
        migrations.AddField(
            model_name='muster',
            name='allWDFound',
            field=models.BooleanField(default=False),
        ),
    ]
