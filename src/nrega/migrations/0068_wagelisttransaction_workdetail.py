# Generated by Django 2.1.3 on 2019-02-15 00:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('nrega', '0067_remove_wagelisttransaction_workdetail'),
    ]

    operations = [
        migrations.AddField(
            model_name='wagelisttransaction',
            name='workDetail',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='nrega.WorkDetail'),
        ),
    ]