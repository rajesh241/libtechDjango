# Generated by Django 2.1.3 on 2019-02-09 10:56

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0044_auto_20190209_1620'),
    ]

    operations = [
        migrations.AlterField(
            model_name='ftotransaction',
            name='dwd',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='nrega.DemandWorkDetail'),
        ),
        migrations.AlterField(
            model_name='wagelisttransaction',
            name='dwd',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='nrega.DemandWorkDetail'),
        ),
    ]
