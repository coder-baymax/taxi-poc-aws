from django.apps import apps
from django.contrib import admin
from django.contrib.admin.sites import AlreadyRegistered
from django.db.models import fields


def add_all_model_to_admin(app_name):
    app = apps.get_app_config(app_name)
    for model_name, model in app.models.items():
        model_admin = type(model_name + "Admin", (admin.ModelAdmin,), {})

        model_admin.list_display = (
            model.admin_list_display
            if hasattr(model, "admin_list_display")
            else tuple([field.name for field in model._meta.fields])
        )
        model_admin.list_display_links = (
            model.admin_list_display_links
            if hasattr(model, "admin_list_display_links")
            else ()
        )
        model_admin.list_editable = (
            model.admin_list_editable if hasattr(model, "admin_list_editable") else ()
        )
        search = []
        for field in model._meta.fields:
            t = type(field)
            if (
                t == fields.IntegerField
                or t == fields.CharField
                or t == fields.FloatField
                or t == fields.TextField
                or t == fields.AutoField
                or t == fields.BigAutoField
                or t == fields.BigIntegerField
            ):
                search.append(field.name)

        model_admin.search_fields = (
            model.admin_search_fields
            if hasattr(model, "admin_search_fields")
            else search
        )

        try:
            admin.site.register(model, model_admin)
        except AlreadyRegistered:
            pass
