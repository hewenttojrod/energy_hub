/**
 * energy_hub module entry point.
 * Exports routes for all energy_hub pages and the navigation sidebar item.
 * 
 * Routes:
 *    energy_hub/home - home
 */

import type { SidebarNavItem, ModuleRoute } from "@app-types/navigation";

export const base_route = "/energy_hub";

export const routes: ModuleRoute[] = [
  { 
    path: base_route, 
    load: () => import("./home") 
  },
  {
    path: `${base_route}/reports`,
    load: () => import("./reports/report_list"),
  },
  {
    path: `${base_route}/reports/:reportId`,
    load: () => import("./reports/report_detail"),
  },
];

export const navItem: SidebarNavItem = {
  id: "energy-hub-home",
  title: "Energy Hub",
  section: "module",
  order: 200,
  children: [
    { 
      id: "energy-hub-home", 
      title: "Home", 
      path: base_route, 
      section: "module", 
      order: 201 
    },
    {
      id: "energy-hub-reports",
      title: "Reports",
      path: `${base_route}/reports`,
      section: "module",
      order: 202,
    },
  ]
};
