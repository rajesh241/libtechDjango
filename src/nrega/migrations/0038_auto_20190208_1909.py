# Generated by Django 2.1.3 on 2019-02-08 13:39

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0037_ftotransaction_dwd'),
    ]

    operations = [
        migrations.AddField(
            model_name='wagelisttransaction',
            name='dwd',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='nrega.DemandWorkDetail'),
        ),
        migrations.AddField(
            model_name='wagelisttransaction',
            name='processDate',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='wagelisttransaction',
            name='referenceNo',
            field=models.CharField(blank=True, max_length=256, null=True),
        ),
        migrations.AddField(
            model_name='wagelisttransaction',
            name='rejectionReason',
            field=models.CharField(blank=True, max_length=4096, null=True),
        ),
        migrations.AddField(
            model_name='wagelisttransaction',
            name='status',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='ftotransaction',
            name='dwd',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='nrega.DemandWorkDetail'),
        ),
    ]