# Generated by Django 2.1.3 on 2018-12-23 05:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0005_crawlqueue_musterdownloadaccuracy'),
    ]

    operations = [
        migrations.AddField(
            model_name='fto',
            name='isRequired',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='wagelist',
            name='isRequired',
            field=models.BooleanField(default=False),
        ),
    ]