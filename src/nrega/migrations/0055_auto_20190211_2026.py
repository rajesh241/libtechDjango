# Generated by Django 2.1.3 on 2019-02-11 14:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0054_workdetail_muster'),
    ]

    operations = [
        migrations.AlterField(
            model_name='workdetail',
            name='workAllocatedDate',
            field=models.DateField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='workdetail',
            name='workDemandDate',
            field=models.DateField(blank=True, db_index=True, null=True),
        ),
    ]
