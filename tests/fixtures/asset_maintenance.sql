-- name: asset_maintenance_alert
-- Get a list of maintenances that are happening between 2 dates and insert the alert to be sent into the database, returns inserted data
with inserted_data as (
insert into alert_users (user_id, asset_maintenance_id, alert_definition_id)
select responsible_id, id, (select id from alert_definition where name = 'maintenances_today') from asset_maintenance
where planned_date_start is not null
and planned_date_start between :date_start and :date_end
and cancelled = False ON CONFLICT ON CONSTRAINT unique_alert DO NOTHING
returning *)
select inserted_data.*, to_jsonb(users.*) as user
from inserted_data
left join users on users.id = inserted_data.user_id;
