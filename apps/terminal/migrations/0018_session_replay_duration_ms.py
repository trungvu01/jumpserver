from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("terminal", "0017_applethost_tinker_version"),
    ]

    operations = [
        migrations.AddField(
            model_name="session",
            name="replay_duration_ms",
            field=models.PositiveBigIntegerField(
                blank=True,
                editable=False,
                null=True,
                verbose_name="Replay duration (ms)",
            ),
        ),
    ]
