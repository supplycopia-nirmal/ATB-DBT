{% macro price_variance2(actual_unit_price, contract_price, contract_ea_price, uom_matches, conv_factor, quantity) %}
    round(
        case 
            when coalesce(try_cast({{ contract_price }} as double), 0) > 0 then
                (try_cast({{ actual_unit_price }} as double) - coalesce(
                    case 
                        when {{ uom_matches }} in ('Y', true, 'true', '1') then try_cast({{ contract_price }} as double)
                        else try_cast({{ contract_ea_price }} as double) * coalesce(try_cast({{ conv_factor }} as double), 1.0)
                    end, 0.0)
                ) * try_cast({{ quantity }} as double)
            else 0.0
        end,
        2
    )
{% endmacro %}
